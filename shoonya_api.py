"""
Shoonya REST wrapper.

Authentication is delegated entirely to :mod:`shoonya_login`; this class only
knows how to send requests with whatever token the session manager hands it,
and how to react when the broker says that token is dead.

The reaction differs by route class, deliberately:

* **Read-only** routes (quotes, books, limits, history) are safe to replay, so
  an explicit session-expiry response triggers one refresh + one retry.
* **Mutating** routes (place / modify / cancel) are **never** replayed
  automatically.  The session is refreshed, but the caller gets an
  :class:`OrderNotReplayedError` and must reconcile against the order book
  before deciding to resubmit.  Silently re-sending an order is how you get
  accidental double fills.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.parse

from dotenv import load_dotenv
from NorenRestApiPy.NorenApi import NorenApi

import ist
from shoonya_login import (
    API_BASE_URL,
    HTTP,
    OrderNotReplayedError,
    PermanentLoginError,
    SessionUnavailableError,
    ShoonyaLoginError,
    TransientLoginError,
    get_session_manager,
    install_token_redaction_filter,
    is_auth_expiry_text,
    redact,
)

load_dotenv()

log = logging.getLogger(__name__)

#: State-changing — never replayed without operator/strategy reconciliation.
#: Every other route is an idempotent read, safe to replay after a refresh.
_MUTATING_ROUTES = frozenset(
    {
        "PlaceOrder",
        "ModifyOrder",
        "CancelOrder",
        "ExitSNOOrder",
        "PositionProductConversion",
    }
)

install_token_redaction_filter()


class ShoonyaApiPy(NorenApi):
    def __init__(self, *, allow_mint: bool | None = None):
        super().__init__(
            host="https://api.shoonya.com/NorenWClientTP/",
            websocket="wss://api.shoonya.com/NorenWSTP/",
        )
        self._session_manager = get_session_manager(allow_mint=allow_mint)
        self.userid = self._session_manager.userid
        self.password = self._session_manager.creds.password
        self.vendor_code = self._session_manager.creds.client_id
        self._applied_token: str | None = None

    # ── session plumbing ─────────────────────────────────────────────────

    @property
    def access_token(self):
        return self._session_manager.token

    @property
    def login_status(self) -> bool:
        """True only when a token actually exists — never a sticky flag."""
        return bool(self._session_manager.token)

    @property
    def websocket_login_status(self) -> bool:
        """
        True when we hold a token usable for websocket auth.

        Note this does *not* mean a socket is connected and acknowledged — only
        :class:`LiveWebSocket` can know that.
        """
        return bool(self._session_manager.token)

    def _apply_session(self, token: str) -> None:
        """
        Push the current token into NorenApi so its inherited helpers work.

        Keyed on the token itself, not on a generation counter: a counter can
        repeat across an invalidate/re-mint cycle, which would skip this call
        and leave NorenApi holding a dead susertoken.
        """
        if token and token != self._applied_token:
            self.set_session(self.userid, self.password, token)
            self._applied_token = token

    def _token_for_request(self) -> str:
        token = self._session_manager.get_access_token()
        self._apply_session(token)
        return token

    # ── public login API (unchanged signatures) ──────────────────────────

    def login_and_search(self, *, force: bool = False):
        """
        Ensure a valid session exists.

        Fast path: a cached token that passes one probe — no browser at all.
        Only an explicit broker rejection causes a real login.
        """
        started = time.time()
        try:
            token = self._session_manager.get_access_token(force_refresh=force)
            self._apply_session(token)
            log.info("Shoonya session ready in %.2fs.", time.time() - started)
            return {"stat": "Ok"}
        except PermanentLoginError as exc:
            log.error("Shoonya login failed permanently: %s", exc)
            return {"stat": "Not_Ok", "emsg": str(exc), "permanent": True}
        except ShoonyaLoginError as exc:
            log.error("Shoonya login failed: %s", exc)
            return {"stat": "Not_Ok", "emsg": str(exc)}

    def ensure_websocket_session(self):
        """
        Guarantee a *validated* token before the websocket authenticates.

        The session manager handles OTP window timing internally.
        """
        try:
            token = self._session_manager.get_access_token()
            self._apply_session(token)
            return {"stat": "Ok"}
        except ShoonyaLoginError as exc:
            return {"stat": "Not_Ok", "emsg": str(exc)}

    def refresh_session_after_expiry(self) -> str:
        """Adopt a newer cached session, or mint one. Used by the websocket."""
        token = self._session_manager.refresh_after_expiry()
        self._apply_session(token)
        return token

    # ── transport ────────────────────────────────────────────────────────

    def _raw_post(self, route, values, token, timeout):
        response = HTTP.post(
            f"{API_BASE_URL}/{route}",
            data="jData=" + json.dumps(values, separators=(",", ":")),
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
        text = (response.text or "").strip()
        if not text:
            raise RuntimeError(f"Shoonya {route} returned empty response (HTTP {response.status_code})")
        try:
            return response.status_code, response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Shoonya {route} returned non-JSON response "
                f"(HTTP {response.status_code}): {redact(text[:300])}"
            ) from exc

    @staticmethod
    def _is_auth_failure(status_code, body) -> bool:
        if status_code in (401, 403):
            return True
        if isinstance(body, dict) and body.get("stat") == "Not_Ok":
            return is_auth_expiry_text(str(body.get("emsg") or ""))
        return False

    def _api_post(self, route, values, *, expect_list=False, timeout=30,
                  warn_on_error=True, no_data_is_empty=False):
        token = self._token_for_request()
        status_code, result = self._raw_post(route, values, token, timeout)

        if self._is_auth_failure(status_code, result):
            mutating = route in _MUTATING_ROUTES
            log.warning("Shoonya session expired during %s (mutating=%s).", route, mutating)
            self._session_manager.invalidate(token)

            if mutating:
                # Refresh so the next call works, but do NOT resend this one:
                # the broker may or may not have accepted it before expiry.
                try:
                    self.refresh_session_after_expiry()
                except ShoonyaLoginError as exc:
                    raise OrderNotReplayedError(
                        f"Shoonya session expired during {route} and could not be "
                        f"re-established ({exc}). The request was NOT replayed — "
                        "reconcile the order book before resubmitting."
                    ) from exc
                raise OrderNotReplayedError(
                    f"Shoonya session expired during {route}. The session has been "
                    "refreshed but the request was NOT replayed — reconcile the order "
                    "book/positions before resubmitting."
                )

            new_token = self.refresh_session_after_expiry()
            status_code, result = self._raw_post(route, values, new_token, timeout)

        if status_code >= 400:
            raise RuntimeError(f"Shoonya {route} failed: {redact(result)}")
        if expect_list and not isinstance(result, list):
            if (
                no_data_is_empty
                and isinstance(result, dict)
                and result.get("stat") == "Not_Ok"
                and "no data" in str(result.get("emsg") or "").lower()
            ):
                # An empty book is answered as an error
                # (`Error Occurred : 5 "no data"`), not as `[]`. For a book it
                # is a definite answer - no positions, no orders - and reading
                # it as a failure blocked every live entry on a flat account.
                return []
            if (
                warn_on_error
                and isinstance(result, dict)
                and result.get("stat") == "Not_Ok"
            ):
                # Previously this became a bare None and the reason vanished.
                # `warn_on_error=False` is for callers that ask on spec and
                # already have the answer elsewhere - see the history warm-up.
                log.warning("Shoonya %s returned an error: %s", route, result.get("emsg"))
            return None
        return result

    def _uid_payload(self, **values):
        payload = {"uid": self.userid}
        payload.update({key: value for key, value in values.items() if value is not None})
        return payload

    def _checked(self, route, result):
        """Return the payload on success; log (not swallow silently) on failure."""
        if isinstance(result, dict) and result.get("stat") == "Ok":
            return result
        if isinstance(result, dict):
            log.warning("Shoonya %s returned an error: %s", route, result.get("emsg"))
        return None

    # ── endpoints ────────────────────────────────────────────────────────

    def get_quotes(self, exchange, token, timeout=30):
        return self._checked(
            "GetQuotes",
            self._api_post("GetQuotes", self._uid_payload(exch=exchange, token=token),
                           timeout=timeout),
        )

    def searchscrip(self, exchange, searchtext):
        if searchtext is None:
            return None
        return self._checked(
            "SearchScrip",
            self._api_post("SearchScrip", self._uid_payload(exch=exchange, stext=searchtext)),
        )

    def get_security_info(self, exchange, token):
        return self._checked(
            "GetSecurityInfo",
            self._api_post("GetSecurityInfo", self._uid_payload(exch=exchange, token=token)),
        )

    def get_time_price_series(self, exchange, token, starttime=None, endtime=None,
                              interval=None, warn_on_error=True):
        if starttime is None:
            # Midnight of the current *IST* day. `time.strftime`/`time.mktime`
            # would read the host's zone, so on a non-IST machine this default
            # silently asked the broker for the wrong session's candles.
            starttime = ist.midnight().timestamp()
        payload = self._uid_payload(
            ordersource="API",
            exch=exchange,
            token=token,
            st=str(starttime),
            et=str(endtime) if endtime is not None else None,
            intrv=str(interval) if interval is not None else None,
        )
        return self._api_post("TPSeries", payload, expect_list=True,
                              warn_on_error=warn_on_error)

    def get_limits(self, product_type=None, segment=None, exchange=None):
        return self._api_post(
            "Limits",
            self._uid_payload(
                actid=self.userid,
                prd=product_type,
                seg=segment,
                exch=exchange,
            ),
        )

    def get_positions(self):
        # Short timeout: the trade loop abandons this call after a few seconds,
        # and a thread left waiting 30s behind it is what piles up.
        return self._api_post(
            "PositionBook",
            self._uid_payload(actid=self.userid),
            expect_list=True,
            timeout=10,
            no_data_is_empty=True,
        )

    def get_order_book(self):
        return self._api_post("OrderBook", self._uid_payload(), expect_list=True,
                              no_data_is_empty=True)

    def get_trade_book(self):
        return self._api_post(
            "TradeBook",
            self._uid_payload(actid=self.userid),
            expect_list=True,
            no_data_is_empty=True,
        )

    def single_order_history(self, orderno):
        return self._api_post(
            "SingleOrdHist",
            self._uid_payload(norenordno=orderno),
            expect_list=True,
            no_data_is_empty=True,
        )

    def place_order(
        self,
        buy_or_sell,
        product_type,
        exchange,
        tradingsymbol,
        quantity,
        discloseqty,
        price_type,
        price=0.0,
        trigger_price=None,
        retention="DAY",
        amo="NO",
        remarks=None,
        bookloss_price=0.0,
        bookprofit_price=0.0,
        trail_price=0.0,
        **kwargs,
    ):
        values = self._uid_payload(
            ordersource="API",
            actid=self.userid,
            exch=exchange,
            tsym=urllib.parse.quote_plus(tradingsymbol),
            qty=str(quantity),
            dscqty=str(discloseqty),
            trantype=buy_or_sell,
            prd=product_type,
            prctyp=price_type,
            prc=str(price),
            trgprc=str(trigger_price) if trigger_price is not None else None,
            ret=retention,
            remarks=remarks,
            amo=amo,
        )
        if product_type == "H":
            values["blprc"] = str(bookloss_price)
            if trail_price != 0.0:
                values["trailprc"] = str(trail_price)
        if product_type == "B":
            values["blprc"] = str(bookloss_price)
            values["bpprc"] = str(bookprofit_price)
            if trail_price != 0.0:
                values["trailprc"] = str(trail_price)
        if kwargs:
            # These were silently dropped before; at least make it visible.
            log.warning("place_order ignoring unsupported arguments: %s", sorted(kwargs))
        return self._checked("PlaceOrder", self._api_post("PlaceOrder", values))

    def cancel_order(self, orderno):
        return self._checked(
            "CancelOrder",
            self._api_post("CancelOrder", self._uid_payload(ordersource="API", norenordno=str(orderno))),
        )


__all__ = [
    "ShoonyaApiPy",
    "OrderNotReplayedError",
    "ShoonyaLoginError",
    "PermanentLoginError",
    "TransientLoginError",
    "SessionUnavailableError",
]
