"""
iVAS OTP Bot — Full-featured Telegram Bot
Button UI: Scan → pilih negara → Add top 10 range → Kirim TXT → Monitor OTP live
"""
import asyncio
import io
import json
import logging
import os
import re
import urllib.parse
from datetime import date

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    Document,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

load_dotenv()

import database
from ivasms import (
    IVASMSClient,
    _country_emoji,
    xlsx_bytes_to_numbers,
    numbers_to_txt,
    parse_cookies,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    p = os.path.join(os.path.dirname(__file__), "config.json")
    if os.path.exists(p):
        try:
            with open(p) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


_cfg = _load_cfg()


def _env(key: str, default: str = "") -> str:
    v = _cfg.get(key, "")
    return str(v) if v else os.getenv(key, default)


BOT_TOKEN = _env("BOT_TOKEN")
_admin_set: set[int] = set()
for _p in _env("ADMIN_CHAT_ID", "0").replace(",", " ").split():
    try:
        _admin_set.add(int(_p))
    except ValueError:
        pass


def is_admin(uid: int) -> bool:
    return bool(_admin_set) and uid in _admin_set


def get_cookies() -> str:
    saved = database.get_setting("ivasms_cookies")
    if saved:
        return saved
    raw = _cfg.get("IVASMS_COOKIES", "")
    if isinstance(raw, dict) and raw:
        return json.dumps(raw)
    if isinstance(raw, str) and raw.strip():
        return raw
    return os.getenv("IVASMS_COOKIES", "")


# ── Global state ──────────────────────────────────────────────────────────────

_otp_task: asyncio.Task | None = None

# Cache hasil scan: list of range dicts sorted by count desc
_scan_cache: list[dict] = []

# Sorted country list: [{country, emoji, total_sms, ranges:[...]}]
_scan_countries: list[dict] = []

SEP = "─" * 30
router = Router()


# ── Keyboards ─────────────────────────────────────────────────────────────────

def main_kb() -> ReplyKeyboardMarkup:
    mon = (
        "🔴 Stop Monitor OTP"
        if (_otp_task and not _otp_task.done())
        else "🟢 Start Monitor OTP"
    )
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📡 Scan Range WA"),     KeyboardButton(text="📦 My Numbers")],
            [KeyboardButton(text="🔄 Return & Cari Baru"), KeyboardButton(text=mon)],
            [KeyboardButton(text="📋 History OTP"),        KeyboardButton(text="ℹ️ Status")],
            [KeyboardButton(text="📤 Upload Nomor Bio"),   KeyboardButton(text="📥 Export Nomor")],
            [KeyboardButton(text="🍪 Set Cookies")],
        ],
        resize_keyboard=True,
        persistent=True,
    )


async def deny(target) -> None:
    txt = "⛔ Akses ditolak!"
    if isinstance(target, CallbackQuery):
        await target.answer(txt, show_alert=True)
    else:
        await target.answer(txt)


# ── /start ────────────────────────────────────────────────────────────────────

@router.message(Command("start"))
async def cmd_start(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    ck  = "🟢 Aktif" if get_cookies() else "🔴 Belum diset"
    mon = "🟢 Aktif" if _otp_task and not _otp_task.done() else "⭕ Mati"
    await msg.answer(
        f"<b>🤖 iVAS OTP Bot</b>\n{SEP}\n"
        f"🍪 Cookies : <b>{ck}</b>\n"
        f"📡 Monitor : <b>{mon}</b>\n"
        f"📱 Stok Bio: <b>{database.count_numbers()} nomor</b>\n"
        f"{SEP}\n"
        f"<b>Alur:</b>\n"
        f"1️⃣  <b>📡 Scan Range WA</b>\n"
        f"2️⃣  Pilih negara yang mau lo tambah\n"
        f"3️⃣  Pilih top 10 range dari negara itu\n"
        f"4️⃣  Bot auto add → kirim TXT otomatis\n"
        f"5️⃣  <b>🟢 Start Monitor OTP</b> → notif real-time!\n"
        f"{SEP}\n<i>Gunakan tombol di bawah 👇</i>",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


# ── Status ────────────────────────────────────────────────────────────────────

@router.message(F.text == "ℹ️ Status")
@router.message(Command("status"))
async def kb_status(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    q   = database.count_by_quality()
    ck  = "🟢 Aktif" if get_cookies() else "🔴 Belum diset"
    mon = "🟢 Aktif" if _otp_task and not _otp_task.done() else "⭕ Mati"
    otps = database.get_today_otps()
    await msg.answer(
        f"<b>ℹ️ Status Bot</b>\n{SEP}\n"
        f"🍪 Cookies iVAS : <b>{ck}</b>\n"
        f"📡 OTP Monitor  : <b>{mon}</b>\n"
        f"📨 OTP hari ini : <b>{len(otps)}</b>\n"
        f"{SEP}\n"
        f"<b>📦 Stok Nomor Bio:</b>\n"
        f"  👑 Bio+LMB : <b>{q.get('bio_lmb',0)}</b>\n"
        f"  ✅ Bio      : <b>{q.get('bio',0)}</b>\n"
        f"  🔵 LMB      : <b>{q.get('lmb',0)}</b>\n"
        f"  ⚪ Standard : <b>{q.get('standard',0)}</b>\n"
        f"  📊 Total    : <b>{database.count_numbers()}</b>",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


# ── Set Cookies ───────────────────────────────────────────────────────────────

@router.message(F.text == "🍪 Set Cookies")
async def kb_setcookies_prompt(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    await msg.answer(
        f"<b>🍪 Set Cookies iVAS</b>\n{SEP}\n"
        f"Gunakan: <code>/setcookies [cookies]</code>\n\n"
        f"<b>Format JSON array:</b>\n"
        f"<code>[{{\"name\":\"XSRF-TOKEN\",\"value\":\"...\"}},{{\"name\":\"ivas_sms_session\",\"value\":\"...\"}}]</code>\n\n"
        f"<b>Cara ambil (PC):</b>\n"
        f"F12 → Application → Cookies → copy XSRF-TOKEN + ivas_sms_session",
        parse_mode="HTML",
    )


@router.message(Command("setcookies"))
async def cmd_setcookies(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2:
        await kb_setcookies_prompt(msg)
        return
    await _process_cookies(msg, parts[1].strip())


async def _process_cookies(msg: Message, raw: str):
    info = await msg.answer("⏳ Ngecek cookies ke iVAS...")
    ok = False
    try:
        async with IVASMSClient(raw) as client:
            ok = await client.login()
            if ok:
                updated = client.get_updated_cookies_str()
                if updated:
                    raw = updated
    except Exception as e:
        logger.error(f"cookies validate: {e}")
    if not ok:
        await info.edit_text(
            f"<b>❌ Cookies Ditolak!</b>\n{SEP}\n"
            f"Tidak valid atau expired. Login ulang ke ivasms.com dan ambil cookies baru.",
            parse_mode="HTML",
        )
        return
    database.set_setting("ivasms_cookies", raw)
    await info.edit_text(
        f"<b>✅ Cookies Disimpan!</b>\n{SEP}\nLogin iVAS sukses ✓  Bot siap! 🚀",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 Scan Range WA Sekarang", callback_data="scan_range")]
        ]),
    )


# ── Scan Range WA — STEP 1: Scan & tampil negara ─────────────────────────────

async def _run_scan(target):
    """
    Scan WA ranges lalu tampilkan pilihan negara via inline buttons.
    target = Message or CallbackQuery
    """
    global _scan_cache, _scan_countries
    is_cb = isinstance(target, CallbackQuery)
    msg   = target.message if is_cb else target

    cookies = get_cookies()
    if not cookies:
        fn = msg.edit_text if is_cb else msg.answer
        await fn("🔴 Cookies belum diset! Tekan 🍪 Set Cookies dulu.", parse_mode="HTML")
        return

    send = msg.edit_text if is_cb else msg.answer
    info = await send(
        f"<b>📡 Scan Range WhatsApp</b>\n{SEP}\n"
        f"⏳ Scanning SMS test history iVAS...\n"
        f"<i>Mengambil 2000 record terbaru (dari 66K+ total)...</i>",
        parse_mode="HTML",
    )

    try:
        async with IVASMSClient(cookies) as client:
            client._apply_cookies()
            ranges = await client.get_wa_active_ranges(limit=2000)
            my_total = await client.get_my_numbers_count()
    except Exception as e:
        logger.error(f"scan error: {e}")
        await info.edit_text(f"❌ Scan gagal: <code>{e}</code>", parse_mode="HTML")
        return

    if not ranges:
        await info.edit_text(
            f"<b>📡 Scan Selesai</b>\n{SEP}\n⚠️ Tidak ada range WA aktif ditemukan.",
            parse_mode="HTML",
        )
        return

    _scan_cache = ranges

    # Build per-country map
    cmap: dict[str, dict] = {}
    for r in ranges:
        c = r["country"]
        if c not in cmap:
            cmap[c] = {"country": c, "total_sms": 0, "ranges": []}
        cmap[c]["total_sms"] += r["count"]
        cmap[c]["ranges"].append(r)

    # Sort countries by total SMS desc, ranges inside each also sorted
    _scan_countries = sorted(
        cmap.values(), key=lambda x: x["total_sms"], reverse=True
    )
    for cd in _scan_countries:
        cd["ranges"].sort(key=lambda r: r["count"], reverse=True)

    total_sms = sum(r["count"] for r in ranges)
    medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, 21)]

    # Header
    lines = [
        "<b>📡 RANGE WhatsApp AKTIF — Pilih Negara</b>",
        SEP,
        f"🔢 <b>{len(ranges)} range aktif</b>  |  ⚡ <b>{total_sms} WA SMS</b>",
        f"📱 My Numbers saat ini: <b>{my_total}/1000</b>",
        SEP,
        "<b>🌍 Top Negara (berdasarkan WA SMS masuk):</b>",
    ]
    for i, cd in enumerate(_scan_countries[:15]):
        em = _country_emoji(cd["country"])
        top_range = cd["ranges"][0]["range_num"] if cd["ranges"] else "?"
        lines.append(
            f"{medals[i]} {em} <b>{cd['country']}</b> — "
            f"⚡ <b>{cd['total_sms']} SMS</b>  ({len(cd['ranges'])} range) top: {top_range}"
        )

    lines += [SEP, "<i>Tap negara di bawah untuk lihat top 10 range-nya ⬇️</i>"]
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:3950] + "\n<i>... dipotong</i>"

    # Build country buttons (max 10, 2 per row)
    top_countries = _scan_countries[:10]
    rows = []
    for i in range(0, len(top_countries), 2):
        row = []
        for j in [i, i+1]:
            if j < len(top_countries):
                cd = top_countries[j]
                em = _country_emoji(cd["country"])
                label = f"{em} {cd['country']} ({cd['total_sms']})"
                row.append(InlineKeyboardButton(text=label, callback_data=f"pc:{j}"))
        rows.append(row)

    rows.append([
        InlineKeyboardButton(text="🔄 Refresh Scan", callback_data="scan_range"),
        InlineKeyboardButton(text="📦 My Numbers", callback_data="my_numbers"),
    ])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await info.edit_text(text, parse_mode="HTML", reply_markup=kb)


@router.message(F.text == "📡 Scan Range WA")
async def kb_scan_range(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    await _run_scan(msg)


@router.callback_query(F.data == "scan_range")
async def cb_scan_range(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer()
    await _run_scan(cb)


# ── STEP 2: Pilih negara → tampil top 10 range dari negara itu ───────────────

@router.callback_query(F.data.startswith("pc:"))
async def cb_pick_country(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer()

    try:
        idx = int(cb.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await cb.message.answer("❌ Data invalid.")
        return

    if not _scan_countries or idx >= len(_scan_countries):
        await cb.message.edit_text(
            "⚠️ Data scan sudah expired. Scan ulang dulu!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📡 Scan Ulang", callback_data="scan_range")]
            ]),
        )
        return

    cd = _scan_countries[idx]
    country  = cd["country"]
    em       = _country_emoji(country)
    ranges   = cd["ranges"]   # sudah sorted by count desc
    top10    = ranges[:10]
    medals   = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, 21)]

    lines = [
        f"<b>📡 Top Range {em} {country}</b>",
        SEP,
        f"⚡ <b>{cd['total_sms']} WA SMS</b> total  |  {len(ranges)} range aktif",
        SEP,
        "<b>Top 10 Range (berdasarkan WA SMS masuk):</b>",
    ]
    for i, r in enumerate(top10):
        lines.append(
            f"{medals[i]} <b>{r['range_num']}</b>"
            f" — ⚡ <b>{r['count']} SMS</b>"
            f" | 🕐 {r['last_seen'][11:16]}"
            f" | ID: <code>{r['termination_id']}</code>"
        )

    lines += [SEP, "<i>Tap tombol di bawah untuk add semua ke My Numbers ⬇️</i>"]
    text = "\n".join(lines)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Add 10 Range {em} {country} + Kirim TXT",
            callback_data=f"ac:{idx}"
        )],
        [
            InlineKeyboardButton(text="◀️ Pilih Negara Lain", callback_data="scan_range"),
            InlineKeyboardButton(text="📦 My Numbers", callback_data="my_numbers"),
        ],
    ])
    await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb)


# ── STEP 3: Add ranges → download XLSX → kirim TXT ───────────────────────────

@router.callback_query(F.data.startswith("ac:"))
async def cb_add_country(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer()

    try:
        idx = int(cb.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await cb.message.answer("❌ Data invalid.")
        return

    if not _scan_countries or idx >= len(_scan_countries):
        await cb.message.edit_text(
            "⚠️ Data scan expired. Scan ulang!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📡 Scan Ulang", callback_data="scan_range")]
            ]),
        )
        return

    cd     = _scan_countries[idx]
    country = cd["country"]
    em      = _country_emoji(country)
    top10   = cd["ranges"][:10]
    medals  = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, 21)]

    cookies = get_cookies()
    if not cookies:
        await cb.message.answer("🔴 Cookies belum diset!")
        return

    info = await cb.message.edit_text(
        f"<b>➕ Add 10 Range {em} {country}</b>\n{SEP}\n⏳ Login ke iVAS...",
        parse_mode="HTML",
    )

    results: list[dict] = []

    try:
        async with IVASMSClient(cookies) as client:
            ok = await client.login()
            if not ok:
                await info.edit_text(
                    f"<b>❌ Login iVAS Gagal</b>\n{SEP}\nCookies expired!",
                    parse_mode="HTML",
                )
                return

            for i, r in enumerate(top10):
                await info.edit_text(
                    f"<b>➕ Add {em} {country} Range</b>\n{SEP}\n"
                    f"⏳ <b>{i+1}/{len(top10)}</b> — {medals[i]} Range <b>{r['range_num']}</b>"
                    f" ({r['count']} WA SMS)\n",
                    parse_mode="HTML",
                )
                res = await client.add_range(r["termination_id"])
                results.append({**r, "ok": res["ok"], "msg": res["message"]})
                await asyncio.sleep(0.8)

            ok_n   = sum(1 for r in results if r["ok"])
            fail_n = len(results) - ok_n

            lines = [
                f"<b>✅ Add Range {em} {country} Selesai</b>",
                SEP,
                f"✅ Berhasil: <b>{ok_n}</b>  ❌ Gagal: <b>{fail_n}</b>",
                SEP,
            ]
            for i, r in enumerate(results):
                icon = "✅" if r["ok"] else "❌"
                lines.append(
                    f"{icon} {medals[i]} {r['range_num']}"
                    f" — <i>{r['msg'][:60]}</i>"
                )
            lines += [SEP, "⏳ Downloading XLSX dari My Numbers..."]
            await info.edit_text("\n".join(lines), parse_mode="HTML")

            await asyncio.sleep(1.5)
            xlsx_data = await client.download_xlsx()

    except Exception as e:
        logger.error(f"add_country error: {e}", exc_info=True)
        await info.edit_text(f"❌ Error: <code>{e}</code>", parse_mode="HTML")
        return

    if not xlsx_data:
        await cb.message.answer("❌ Gagal download XLSX dari iVAS.")
        return

    numbers = xlsx_bytes_to_numbers(xlsx_data)
    if not numbers:
        await cb.message.answer("⚠️ XLSX kosong atau format tidak dikenal.")
        return

    txt       = numbers_to_txt(numbers)
    today_str = date.today().strftime("%Y%m%d")
    fname     = f"my_numbers_{country.replace(' ','_')}_{today_str}.txt"

    await info.edit_text(
        "\n".join(lines[:-1])
        + f"\n\n✅ <b>{len(numbers)} nomor</b> siap dikirim!",
        parse_mode="HTML",
    )
    await cb.message.answer_document(
        document=BufferedInputFile(txt, filename=fname),
        caption=(
            f"📱 <b>My Numbers — {em} {country}</b>\n{SEP}\n"
            f"✅ Range ditambah : <b>{ok_n}</b>\n"
            f"📊 Total nomor    : <b>{len(numbers)}</b>\n"
            f"🌍 Range added   : "
            + ", ".join(r["range_num"] for r in results if r["ok"])
        ),
        parse_mode="HTML",
    )


# ── Return & Cari Baru ───────────────────────────────────────────────────────

@router.message(F.text == "🔄 Return & Cari Baru")
async def kb_return_refresh(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    cookies = get_cookies()
    if not cookies:
        await msg.answer("🔴 Cookies belum diset! Tekan 🍪 Set Cookies dulu.")
        return

    # Ambil jumlah My Numbers sekarang
    info = await msg.answer(
        f"<b>🔄 Return & Cari Range Baru</b>\n{SEP}\n⏳ Cek My Numbers...",
        parse_mode="HTML",
    )
    try:
        async with IVASMSClient(cookies) as client:
            client._apply_cookies()
            total = await client.get_my_numbers_count()
    except Exception as e:
        total = -1

    count_text = f"<b>{total}/1000 nomor</b>" if total >= 0 else "<i>(gagal cek)</i>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✅ Ya, Return Semua & Cari Range Baru",
            callback_data="confirm_return_all"
        )],
        [InlineKeyboardButton(text="❌ Batal", callback_data="cancel_return")],
    ])
    await info.edit_text(
        f"<b>⚠️ Konfirmasi Return Semua Nomor</b>\n{SEP}\n"
        f"My Numbers saat ini: {count_text}\n\n"
        f"Bot akan:\n"
        f"1️⃣  Return <b>semua nomor</b> ke sistem iVAS\n"
        f"2️⃣  Langsung scan range WA terbaru\n"
        f"3️⃣  Lo pilih negara → bot add 10 range baru\n\n"
        f"<b>❗ Pastikan tidak ada OTP yang sedang ditunggu!</b>\n"
        f"{SEP}\n"
        f"<i>Lanjut?</i>",
        parse_mode="HTML",
        reply_markup=kb,
    )


@router.callback_query(F.data == "cancel_return")
async def cb_cancel_return(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer("❌ Dibatalkan")
    await cb.message.edit_text(
        f"<b>❌ Return Dibatalkan</b>\n{SEP}\nMy Numbers tidak berubah.",
        parse_mode="HTML",
    )


@router.callback_query(F.data == "confirm_return_all")
async def cb_confirm_return_all(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer("⏳ Memproses...")

    cookies = get_cookies()
    if not cookies:
        await cb.message.edit_text("🔴 Cookies belum diset!")
        return

    info = await cb.message.edit_text(
        f"<b>🔄 Return Semua Nomor</b>\n{SEP}\n⏳ Login ke iVAS...",
        parse_mode="HTML",
    )

    try:
        async with IVASMSClient(cookies) as client:
            ok = await client.login()
            if not ok:
                await info.edit_text(
                    f"<b>❌ Login iVAS Gagal</b>\n{SEP}\nCookies expired!",
                    parse_mode="HTML",
                )
                return

            await info.edit_text(
                f"<b>🔄 Return Semua Nomor</b>\n{SEP}\n"
                f"⏳ Mengirim perintah <b>Return All</b> ke iVAS...",
                parse_mode="HTML",
            )
            result = await client.bulk_return_all()

    except Exception as e:
        logger.error(f"confirm_return_all: {e}", exc_info=True)
        await info.edit_text(f"❌ Error: <code>{e}</code>", parse_mode="HTML")
        return

    if not result["ok"]:
        await info.edit_text(
            f"<b>❌ Return Gagal</b>\n{SEP}\n"
            f"<code>{result['message']}</code>",
            parse_mode="HTML",
        )
        return

    await info.edit_text(
        f"<b>✅ Return Berhasil!</b>\n{SEP}\n"
        f"🗑️ <b>{result['count']} nomor</b> dikembalikan ke sistem iVAS\n"
        f"📭 My Numbers sekarang: <b>0/1000</b>\n"
        f"{SEP}\n"
        f"⏳ Langsung scan range WA terbaru...",
        parse_mode="HTML",
    )

    # Tunggu sebentar biar iVAS update state-nya
    await asyncio.sleep(1.5)

    # Auto-trigger scan untuk langsung cari range baru
    await _run_scan(cb)


# ── My Numbers ────────────────────────────────────────────────────────────────

async def _show_my_numbers(msg: Message, edit: bool = False):
    cookies = get_cookies()
    if not cookies:
        fn = msg.edit_text if edit else msg.answer
        await fn("🔴 Cookies belum diset!")
        return
    fn   = msg.edit_text if edit else msg.answer
    info = await fn(
        f"<b>📦 My Numbers iVAS</b>\n{SEP}\n⏳ Mengambil data...",
        parse_mode="HTML",
    )
    try:
        async with IVASMSClient(cookies) as client:
            client._apply_cookies()
            total    = await client.get_my_numbers_count()
            xlsx_data = await client.download_xlsx()
    except Exception as e:
        await info.edit_text(f"❌ Error: <code>{e}</code>", parse_mode="HTML")
        return

    numbers = xlsx_bytes_to_numbers(xlsx_data) if xlsx_data else []
    used    = total if total >= 0 else len(numbers)
    pct     = int(used / 1000 * 100)
    bar     = "█" * (pct // 10) + "░" * (10 - pct // 10)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Download TXT Nomor", callback_data="dl_txt")],
        [
            InlineKeyboardButton(text="📡 Scan Range WA", callback_data="scan_range"),
            InlineKeyboardButton(text="🔄 Refresh", callback_data="my_numbers"),
        ],
    ])
    await info.edit_text(
        f"<b>📦 My Numbers iVAS</b>\n{SEP}\n"
        f"📊 <b>{used}/1000</b>  [{bar}] {pct}%\n"
        f"📋 Terbaca XLSX : <b>{len(numbers)} nomor</b>\n"
        f"{'🔴 PENUH!' if used >= 1000 else f'🟡 Tersisa {1000-used} slot'}\n"
        f"{SEP}\n<i>Tap Download TXT untuk export nomor ⬇️</i>",
        parse_mode="HTML",
        reply_markup=kb,
    )


@router.message(F.text == "📦 My Numbers")
async def kb_my_numbers(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    await _show_my_numbers(msg)


@router.callback_query(F.data == "my_numbers")
async def cb_my_numbers(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer()
    await _show_my_numbers(cb.message, edit=True)


@router.callback_query(F.data == "dl_txt")
async def cb_dl_txt(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        await deny(cb)
        return
    await cb.answer("⏳ Downloading...")
    cookies = get_cookies()
    if not cookies:
        await cb.message.answer("🔴 Cookies belum diset!")
        return
    try:
        async with IVASMSClient(cookies) as client:
            client._apply_cookies()
            xlsx_data = await client.download_xlsx()
    except Exception as e:
        await cb.message.answer(f"❌ Error: <code>{e}</code>", parse_mode="HTML")
        return
    if not xlsx_data:
        await cb.message.answer("❌ Gagal download XLSX.")
        return
    numbers = xlsx_bytes_to_numbers(xlsx_data)
    if not numbers:
        await cb.message.answer("⚠️ XLSX kosong.")
        return
    today_str = date.today().strftime("%Y%m%d")
    await cb.message.answer_document(
        document=BufferedInputFile(numbers_to_txt(numbers),
                                   filename=f"my_numbers_{today_str}.txt"),
        caption=f"📱 <b>My Numbers iVAS</b> — {today_str}\n📊 <b>{len(numbers)} nomor</b>",
        parse_mode="HTML",
    )


# ── Upload Nomor Bio ──────────────────────────────────────────────────────────

@router.message(F.text == "📤 Upload Nomor Bio")
async def kb_upload_bio(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    await msg.answer(
        f"<b>📤 Upload Nomor Bio</b>\n{SEP}\n"
        f"Kirim file <b>.txt</b> sekarang!\n\n"
        f"Format yang didukung:\n"
        f"1️⃣  Nomor per baris: <code>+628123456789</code>\n"
        f"2️⃣  File hasil cekbio (auto-detect kualitas)\n\n"
        f"Kualitas: 👑 Bio+LMB | ✅ Bio | 🔵 LMB | ⚪ Standard",
        parse_mode="HTML",
    )


@router.message(F.document)
async def handle_document(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    doc: Document = msg.document
    if not doc.file_name or not doc.file_name.lower().endswith(".txt"):
        await msg.answer("❌ Hanya file <b>.txt</b> yang diterima!", parse_mode="HTML")
        return
    info = await msg.answer(f"⏳ Memproses <b>{doc.file_name}</b>...", parse_mode="HTML")
    try:
        file = await msg.bot.get_file(doc.file_id)
        buf  = io.BytesIO()
        await msg.bot.download_file(file.file_path, destination=buf)
        content = buf.getvalue().decode("utf-8", errors="replace")
    except Exception as e:
        await info.edit_text(f"❌ Gagal download file: <code>{e}</code>", parse_mode="HTML")
        return

    if _is_cekbio(content):
        entries = _parse_cekbio(content)
        if not entries:
            await info.edit_text("⚠️ File cekbio terdeteksi tapi tidak ada nomor valid.")
            return
        added, skipped = database.add_numbers_with_quality(entries)
        qc: dict[str, int] = {}
        for _, q in entries:
            qc[q] = qc.get(q, 0) + 1
        await info.edit_text(
            f"<b>✅ File Cekbio Diproses!</b>\n{SEP}\n"
            f"📁 <code>{doc.file_name}</code>\n"
            f"✅ Ditambah : <b>{added}</b>  ⏭ Duplikat: <b>{skipped}</b>\n"
            f"{SEP}\n"
            f"  👑 Bio+LMB : <b>{qc.get('bio_lmb',0)}</b>\n"
            f"  ✅ Bio      : <b>{qc.get('bio',0)}</b>\n"
            f"  🔵 LMB      : <b>{qc.get('lmb',0)}</b>\n"
            f"  ⚪ Standard : <b>{qc.get('standard',0)}</b>",
            parse_mode="HTML",
        )
    else:
        numbers = []
        for line in content.splitlines():
            m = re.search(r'\+?\d{7,15}', line.strip())
            if m:
                numbers.append((m.group(), "standard"))
        if not numbers:
            await info.edit_text("⚠️ Tidak ada nomor valid.\nFormat: <code>+628...</code>", parse_mode="HTML")
            return
        added, skipped = database.add_numbers_with_quality(numbers)
        await info.edit_text(
            f"<b>✅ Nomor Diproses!</b>\n{SEP}\n"
            f"📁 <code>{doc.file_name}</code>\n"
            f"✅ Ditambah : <b>{added}</b>  ⏭ Duplikat: <b>{skipped}</b>\n"
            f"📊 Total    : <b>{database.count_numbers()}</b>",
            parse_mode="HTML",
        )


def _is_cekbio(content: str) -> bool:
    u = content.upper()
    return "HASIL CEK BIO" in u or "NOMOR DENGAN BIO" in u or "NOMOR TANPA BIO" in u


def _parse_cekbio(content: str) -> list[tuple[str, str]]:
    entries  = []
    m_bio    = re.search(r'\[\s*NOMOR DENGAN BIO',     content, re.IGNORECASE)
    m_nobio  = re.search(r'\[\s*NOMOR TANPA BIO',      content, re.IGNORECASE)
    m_end    = re.search(r'\[\s*NOMOR TIDAK TERDAFTAR', content, re.IGNORECASE)
    phone_re = re.compile(r'\+\d{7,15}')
    lmb_re   = re.compile(r'Low Meta Business', re.IGNORECASE)

    if m_bio:
        end = m_nobio.start() if m_nobio else (m_end.start() if m_end else len(content))
        for block in re.split(r'\[\d+\]', content[m_bio.start():end]):
            phones = phone_re.findall(block)
            if not phones:
                continue
            has_lmb = bool(lmb_re.search(block))
            bio_ln  = re.search(r'Bio:\s*(.+)', block)
            has_bio = bool(bio_ln and bio_ln.group(1).strip())
            q = "bio_lmb" if (has_lmb and has_bio) else ("lmb" if has_lmb else "bio")
            for p in phones:
                entries.append((p, q))

    if m_nobio:
        end = m_end.start() if m_end else len(content)
        for line in content[m_nobio.start():end].splitlines():
            phones = phone_re.findall(line)
            if not phones:
                continue
            q = "lmb" if lmb_re.search(line) else "standard"
            for p in phones:
                entries.append((p, q))
    return entries


# ── Export nomor bio ──────────────────────────────────────────────────────────

@router.message(F.text == "📥 Export Nomor")
async def kb_export_nomor(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    numbers = database.get_all_numbers_for_export()
    if not numbers:
        await msg.answer("📭 Stok bio kosong! Upload dulu via 📤 Upload Nomor Bio.")
        return
    q_lbl = {"bio_lmb":"Bio+LMB","bio":"Bio","lmb":"LMB","standard":"Std"}
    lines = [f"{n} [{q_lbl.get(q,q)}]" for n, q in numbers]
    today_str = date.today().strftime("%Y%m%d")
    await msg.answer_document(
        document=BufferedInputFile(
            "\n".join(lines).encode("utf-8"),
            filename=f"stok_bio_{today_str}_{len(numbers)}pcs.txt"
        ),
        caption=f"📥 <b>Stok Bio</b> — {today_str}\n📊 <b>{len(numbers)} nomor</b>",
        parse_mode="HTML",
    )


# ── History OTP ───────────────────────────────────────────────────────────────

@router.message(F.text == "📋 History OTP")
@router.message(Command("history"))
async def kb_history(msg: Message):
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    otps = database.get_today_otps()
    if not otps:
        await msg.answer(
            f"<b>📋 History OTP Hari Ini</b>\n{SEP}\n"
            f"Belum ada OTP masuk.\n"
            f"Pastikan <b>🟢 Start Monitor OTP</b> aktif!",
            parse_mode="HTML",
        )
        return
    lines = [f"<b>📋 OTP Hari Ini — {len(otps)} masuk</b>\n{SEP}"]
    for o in otps[-20:]:
        wkt = o["seen_at"][11:16] if o.get("seen_at") else "?"
        lines.append(
            f"🕐 <i>{wkt}</i>  📱 <code>{o['phone_number']}</code>\n"
            f"🔑 <code>{o['otp_message']}</code>"
        )
    text = "\n\n".join(lines)
    if len(text) > 4000:
        text = text[:3950] + "\n<i>... dipotong</i>"
    await msg.answer(text, parse_mode="HTML")


# ── OTP Monitor — Socket Live ─────────────────────────────────────────────────

@router.message(F.text.startswith("🟢 Start Monitor OTP"))
async def kb_start_monitor(msg: Message):
    global _otp_task
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    if _otp_task and not _otp_task.done():
        await msg.answer(
            "⚠️ Monitor OTP sudah aktif!\nTekan 🔴 Stop Monitor OTP untuk matikan.",
            reply_markup=main_kb(),
        )
        return
    cookies = get_cookies()
    if not cookies:
        await msg.answer("🔴 Set cookies iVAS dulu!", reply_markup=main_kb())
        return
    _otp_task = asyncio.create_task(_monitor_loop(msg.bot, msg.chat.id, cookies))
    await msg.answer(
        f"<b>🟢 Monitor OTP Aktif!</b>\n{SEP}\n"
        f"📡 Connecting ke iVAS live SMS socket...\n"
        f"🔔 Setiap OTP (WA/Telegram/dll) yang masuk ke My Numbers\n"
        f"   akan <b>langsung dikirim ke sini secara real-time!</b>\n\n"
        f"<i>Tekan 🔴 Stop Monitor OTP untuk berhenti.</i>",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


@router.message(F.text.startswith("🔴 Stop Monitor OTP"))
async def kb_stop_monitor(msg: Message):
    global _otp_task
    if not is_admin(msg.from_user.id):
        await deny(msg)
        return
    if _otp_task and not _otp_task.done():
        _otp_task.cancel()
        try:
            await _otp_task
        except asyncio.CancelledError:
            pass
        _otp_task = None
        await msg.answer(
            f"<b>🔴 Monitor OTP Dihentikan</b>\n{SEP}\nSocket disconnected.",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    else:
        await msg.answer("⭕ Monitor OTP sudah mati.", reply_markup=main_kb())


# ── Socket Monitor Loop ───────────────────────────────────────────────────────

async def _monitor_loop(bot: Bot, chat_id: int, cookies_raw: str):
    """
    Background task:
    1. Ambil socket params fresh dari halaman /portal/live/my_sms
    2. Connect socket.io ke ivasms.com:2087/livesms
    3. Listen untuk event — forward OTP ke Telegram
    4. Auto reconnect kalau putus
    """
    try:
        import socketio as sio_lib
    except ImportError:
        await bot.send_message(chat_id, "❌ python-socketio tidak terinstall!\nJalankan: pip install python-socketio[asyncio]")
        return

    RECONNECT_DELAY = 20
    attempt = 0

    while True:
        attempt += 1
        logger.info(f"OTP monitor: attempt #{attempt}")
        sio = sio_lib.AsyncClient(reconnection=False, logger=False, engineio_logger=False)
        connected_ev = asyncio.Event()

        try:
            # Step 1: Ambil socket params fresh tiap connect
            async with IVASMSClient(cookies_raw) as client:
                params = await client.get_live_sms_socket_params()

            if not params:
                await bot.send_message(
                    chat_id,
                    f"⚠️ Gagal ambil socket params dari iVAS (attempt #{attempt}).\n"
                    f"Kemungkinan cookies expired. Retry {RECONNECT_DELAY}s...",
                )
                await asyncio.sleep(RECONNECT_DELAY)
                continue

            token      = params["token"]
            user_hash  = params["user"]
            event_name = params["event_name"]
            cookies_d  = parse_cookies(cookies_raw)
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies_d.items())

            logger.info(f"OTP monitor: user={user_hash[:8]}  event={event_name[:20]}...")

            # Step 2: Setup event handlers

            @sio.event(namespace="/livesms")
            async def connect():
                connected_ev.set()
                logger.info("OTP monitor: socket connected ✅")
                await bot.send_message(
                    chat_id,
                    f"🟢 <b>Monitor OTP Terhubung!</b>\n"
                    f"Menunggu SMS masuk ke <b>My Numbers</b>...\n"
                    f"<i>Semua SMS akan diforward ke sini (filter WA prioritas).</i>",
                    parse_mode="HTML",
                )

            @sio.event(namespace="/livesms")
            async def disconnect():
                logger.warning("OTP monitor: socket disconnected!")

            @sio.on(event_name, namespace="/livesms")
            async def on_sms(data):
                """
                Data fields yang terkonfirmasi dari iVAS JS:
                  data.recipient    = nomor penerima (My Numbers)
                  data.originator   = pengirim (WhatsApp, Telegram, +44xxx, dll)
                  data.message      = isi SMS (OTP ada di sini)
                  data.range        = nama range (IVORY COAST 2930)
                  data.country_iso  = kode negara (CI)
                  data.termination_id
                  data.client_revenue  (0=unpaid)
                  data.limit
                """
                # Log semua data untuk debugging
                logger.info(f"LiveSMS event: {json.dumps(data, ensure_ascii=False)[:500]}")
                try:
                    await _forward_sms(bot, chat_id, data)
                except Exception as exc:
                    logger.error(f"forward_sms error: {exc}", exc_info=True)

            # Step 3: Connect
            conn_url = (
                f"https://ivasms.com:2087/livesms"
                f"?token={urllib.parse.quote(token, safe='')}"
                f"&user={user_hash}"
            )
            await sio.connect(
                conn_url,
                transports=["websocket"],
                headers={"Cookie": cookie_str},
                wait_timeout=15,
            )

            # Tunggu connected confirmation (timeout 20s)
            try:
                await asyncio.wait_for(connected_ev.wait(), timeout=20)
            except asyncio.TimeoutError:
                logger.warning("OTP monitor: connect timeout, socket tidak respond")
                await sio.disconnect()
                raise Exception("Socket connection timeout — iVAS tidak merespons dalam 20s")

            # Jaga koneksi tetap berjalan sampai disconnect atau cancel
            await sio.wait()

            # Kalau sampai sini artinya socket disconnect
            logger.warning("OTP monitor: sio.wait() ended — disconnected")
            raise Exception("Socket terputus dari server iVAS")

        except asyncio.CancelledError:
            logger.info("OTP monitor: cancelled by user ✅")
            try:
                await sio.disconnect()
            except Exception:
                pass
            return  # ← keluar loop, tugas selesai

        except Exception as exc:
            logger.error(f"OTP monitor loop error: {exc}")
            try:
                await sio.disconnect()
            except Exception:
                pass
            await bot.send_message(
                chat_id,
                f"⚠️ <b>Monitor OTP Terputus</b>\n"
                f"Error: <code>{str(exc)[:150]}</code>\n"
                f"🔄 Reconnect dalam <b>{RECONNECT_DELAY}s</b>... (attempt #{attempt+1})",
                parse_mode="HTML",
            )
            await asyncio.sleep(RECONNECT_DELAY)


async def _forward_sms(bot: Bot, chat_id: int, data: dict):
    """
    Process incoming socket event dan forward ke Telegram.
    Confirmed data fields dari iVAS JS source:
      data.recipient, data.originator, data.message,
      data.range, data.country_iso, data.termination_id,
      data.client_revenue, data.limit
    """
    recipient   = str(data.get("recipient", "?")).strip()
    originator  = str(data.get("originator", "?")).strip()
    message     = str(data.get("message", "")).strip()
    rng_name    = str(data.get("range", "")).strip()
    country_iso = str(data.get("country_iso", "")).strip()
    revenue     = data.get("client_revenue", 0)
    paid        = revenue != 0 if revenue is not None else False

    if not message:
        logger.debug("Empty message, skipping")
        return

    # Dedup check: jangan kirim duplikat
    if database.is_otp_seen(recipient, message):
        logger.debug(f"Duplicate OTP skipped: {recipient}")
        return
    database.mark_otp_seen(recipient, message)

    # Extract OTP code dari pesan (4-8 digit berturut-turut)
    otp_match = re.search(r'\b(\d{4,8})\b', message)
    otp_code  = otp_match.group(1) if otp_match else None

    # Country emoji dari ISO code
    em = ""
    if country_iso and len(country_iso) == 2:
        em = "".join(chr(ord(c) + 127397) for c in country_iso.upper()) + " "
    elif rng_name:
        m = re.match(r'^([A-Za-z ]+)\s+\d', rng_name)
        if m:
            em = _country_emoji(m.group(1).strip()) + " "

    # Deteksi jenis SMS
    orig_up = originator.upper().replace("+", "").replace(" ", "")
    is_wa = (
        "WHATSAPP" in orig_up
        or orig_up in ("WA", "WAPP")
        or "whatsapp" in message.lower()
        or "is your whatsapp" in message.lower()
        or re.search(r'kode whatsapp', message, re.IGNORECASE) is not None
    )
    is_tg = "TELEGRAM" in orig_up or "telegram" in message.lower()

    if is_wa:
        type_icon = "🟢 <b>WhatsApp OTP</b>"
    elif is_tg:
        type_icon = "📱 <b>Telegram OTP</b>"
    else:
        type_icon = f"📨 <b>SMS dari {originator}</b>"

    paid_icon = "💰 Paid" if paid else "⚪ Unpaid"

    notif = (
        f"🔔 {type_icon} Masuk!\n"
        f"{'─'*26}\n"
        f"📱 Nomor  : <code>{recipient}</code>\n"
        f"🌍 Range  : {em}<b>{rng_name}</b>\n"
        f"📨 Sender : <b>{originator}</b>\n"
    )

    if otp_code:
        notif += f"🔑 <b>OTP CODE : <code>{otp_code}</code></b>\n"

    notif += (
        f"{'─'*26}\n"
        f"💬 Pesan:\n<code>{message[:300]}</code>\n"
        f"{'─'*26}\n"
        f"{paid_icon}  |  Range: {rng_name}"
    )

    await bot.send_message(chat_id, notif, parse_mode="HTML")
    logger.info(f"SMS forwarded: {recipient} | {originator} | otp={otp_code} | paid={paid}")


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN tidak ditemukan! Isi di config.json atau env var BOT_TOKEN.")
        return
    if not _admin_set:
        logger.warning("ADMIN_CHAT_ID tidak diset — bot akan tolak semua user!")

    database.init_db()

    bot = Bot(token=BOT_TOKEN)
    dp  = Dispatcher()
    dp.include_router(router)

    logger.info(f"🤖 iVAS OTP Bot starting... Admin IDs: {_admin_set}")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
