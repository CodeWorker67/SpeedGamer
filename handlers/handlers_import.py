from aiogram import Router, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InputMediaPhoto

from bot import bot, x3
from X3 import SUBSCRIPTION_SLOTS, panel_username_for_telegram_slot
from keyboard import (
    BTN_BACK,
    create_kb,
    keyboard_import_after_album,
    keyboard_import_app,
    keyboard_import_os,
    keyboard_import_slots,
    keyboard_import_sub,
)
from lexicon import lexicon
from utils.custom_emoji import emojify
from utils.menu_photos import import_photos
from utils.menu_ui import edit_or_send_photo

router: Router = Router()

OS_CALLBACKS = {'import_android', 'import_ios', 'import_windows', 'import_macos'}
_IMPORT_SUB_SLOTS = {slot for slot, _ in SUBSCRIPTION_SLOTS}
_OS_KEYS = ('android', 'ios', 'windows', 'macos')
_APP_KEYS = ('incy', 'happ', 'v2')

OS_DISPLAY = {
    'android': '🤖 Android',
    'ios': '🍎 iOS',
    'windows': '🖥️ Windows',
    'macos': '🍏 MacOS',
}

APP_DISPLAY = {
    'incy': '🔥 INCY',
    'happ': '⭐️ Happ',
    'v2': '📡 V2raytun',
}

IMPORT_URLS = {
    'android': {
        'incy': {
            'url_app': 'https://play.google.com/store/apps/details?id=llc.itdev.incy',
        },
        'happ': {
            'url_app': 'https://play.google.com/store/apps/details?id=com.happproxy',
            'url_import': 'happ://add/{sub_link}',
        },
        'v2': {
            'url_app': 'https://play.google.com/store/apps/details?id=com.v2raytun.android',
            'url_import': 'v2raytun://import/{sub_link}',
        },
    },
    'ios': {
        'incy': {
            'url_app': 'https://apps.apple.com/ru/app/incy/id6756943388',
        },
        'happ': {
            'url_app': 'https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973',
            'url_import': 'happ://add/{sub_link}',
        },
        'v2': {
            'url_app': 'https://apps.apple.com/app/v2raytun/id6476628951',
            'url_import': 'v2raytun://import/{sub_link}',
        },
    },
    'windows': {
        'incy': {
            'url_app': 'https://github.com/INCY-DEV/incy-platforms/releases/latest/download/incy-windows-setup.exe',
        },
        'happ': {
            'url_app': 'https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe',
            'url_import': 'happ://add/{sub_link}',
        },
        'v2': {
            'url_app': 'https://v2raytun.com/',
            'url_import': 'v2raytun://import/{sub_link}',
        },
    },
    'macos': {
        'incy': {
            'url_app': 'https://github.com/INCY-DEV/incy-platforms/releases/latest/download/incy-macos-arm64.dmg',
        },
        'happ': {
            'url_app': 'https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973',
            'url_import': 'happ://add/{sub_link}',
        },
        'v2': {
            'url_app': 'https://apps.apple.com/ru/app/v2raytun/id6476628951',
            'url_import': 'v2raytun://import/{sub_link}',
        },
    },
}


def _active_slot_buttons(slots) -> list[tuple[str, str]]:
    return [(slot, label) for slot, label, *_ in slots]


async def _show_slot_select(callback: CallbackQuery) -> None:
    slots = await x3.active_subscription_slots(callback.from_user.id)
    subscriptions = _active_slot_buttons(slots)
    if not subscriptions:
        await edit_or_send_photo(
            callback,
            "faq",
            lexicon['no_sub'],
            create_kb(1, connect_vpn=BTN_BACK),
        )
        return
    await edit_or_send_photo(
        callback,
        "faq",
        lexicon['import_start'],
        keyboard_import_slots(subscriptions),
    )


async def _finish_import(callback: CallbackQuery, os_key: str, app_key: str, slot: str) -> None:
    labels = dict(SUBSCRIPTION_SLOTS)
    label = labels.get(slot, slot)
    username = panel_username_for_telegram_slot(callback.from_user.id, slot)
    sub_url = await x3.sublink(username)

    if not sub_url:
        await edit_or_send_photo(
            callback,
            "faq",
            '❌ Не удалось получить ссылку. Обратитесь в поддержку.',
            create_kb(1, back_to_main=BTN_BACK),
        )
        return

    urls = IMPORT_URLS[os_key][app_key]
    url_app = urls['url_app']

    if app_key == 'incy':
        lexicon_key = 'import_end_incy'
    elif app_key == 'happ':
        lexicon_key = 'import_end_happ'
    else:
        lexicon_key = 'import_end_v2'

    caption = emojify(lexicon[lexicon_key].format(
        os=OS_DISPLAY[os_key],
        app=APP_DISPLAY[app_key],
        label=label,
        url_app=url_app,
        url_import=sub_url,
    ))
    photos = import_photos(app_key)
    media = [InputMediaPhoto(media=file_id) for file_id in photos]
    media[0] = InputMediaPhoto(media=photos[0], caption=caption, parse_mode='HTML')

    try:
        await callback.message.delete()
    except TelegramBadRequest:
        pass

    await bot.send_media_group(callback.message.chat.id, media=media)
    await bot.send_message(
        callback.message.chat.id,
        "Если нужно, вернитесь в меню:",
        reply_markup=keyboard_import_after_album(),
    )


@router.callback_query(F.data == 'import')
async def import_select_slot(callback: CallbackQuery):
    await callback.answer()
    await _show_slot_select(callback)


@router.callback_query(F.data.regexp(r'^import_slot_(main|3|10|white)$'))
async def import_select_os(callback: CallbackQuery):
    await callback.answer()
    slot = callback.data.removeprefix('import_slot_')
    await edit_or_send_photo(
        callback,
        "faq",
        lexicon['import_select_os'],
        keyboard_import_os(slot),
    )


@router.callback_query(F.data.in_(OS_CALLBACKS))
async def import_select_app_legacy(callback: CallbackQuery):
    """Старые сообщения без выбранной подписки — сначала слот."""
    await callback.answer()
    await _show_slot_select(callback)


@router.callback_query(
    F.data.regexp(r'^import_(android|ios|windows|macos)_(main|3|10|white)$')
)
async def import_select_app(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split('_')
    os_key = parts[1]
    slot = parts[2]
    await edit_or_send_photo(
        callback,
        "faq",
        lexicon['import_select_app'],
        keyboard_import_app(f'import_{os_key}', slot=slot, back_callback=f'import_slot_{slot}'),
    )


@router.callback_query(
    F.data.startswith('import_') &
    (F.data.endswith('_incy') | F.data.endswith('_happ') | F.data.endswith('_v2'))
)
async def import_select_sub_legacy(callback: CallbackQuery):
    """Старые сообщения: после приложения ещё выбирали подписку."""
    await callback.answer()
    slots = await x3.active_subscription_slots(callback.from_user.id)
    subscriptions = _active_slot_buttons(slots)
    if not subscriptions:
        await edit_or_send_photo(
            callback,
            "faq",
            lexicon['no_sub'],
            create_kb(1, back_to_main=BTN_BACK),
        )
        return
    await edit_or_send_photo(
        callback,
        "faq",
        lexicon['import_select_sub'],
        keyboard_import_sub(callback.data, subscriptions),
    )


@router.callback_query(
    F.data.startswith('import_') &
    F.data.split('_')[-1].in_(_IMPORT_SUB_SLOTS)
)
async def import_end(callback: CallbackQuery):
    parts = callback.data.split('_')
    if len(parts) < 4:
        await callback.answer()
        return

    os_key = parts[1]
    app_key = parts[2]
    slot = parts[3]
    if os_key not in OS_DISPLAY or app_key not in APP_DISPLAY:
        await callback.answer()
        return

    await callback.answer()
    await _finish_import(callback, os_key, app_key, slot)
