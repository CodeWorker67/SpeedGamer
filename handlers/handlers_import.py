import urllib.parse

from aiogram import Router, F
from aiogram.types import CallbackQuery, InputMediaPhoto

from bot import x3
from keyboard import (
    keyboard_import_os,
    keyboard_import_app,
    keyboard_import_sub,
    keyboard_import_end,
    create_kb,
)
from lexicon import lexicon

router: Router = Router()

OS_CALLBACKS = {'import_android', 'import_ios', 'import_windows', 'import_macos'}

INCY_PHOTOS = [
    'AgACAgQAAxkBAAFc_vJqQlS3lOcGNBGiNRJCcxdzwuf8swACAg9rG-RnGFIv88tguls85wEAAwIAA3gAAzwE',
    'AgACAgQAAxkBAAFc_0JqQlS_sBd8CPp_bsiFrlDd3pBRsQACAw9rG-RnGFIarCYVPhCt8gEAAwIAA3gAAzwE',
]

HAPP_PHOTOS = [
    'AgACAgIAAxkBAAEPMfNpuuFYB037vCUdedfpqS5ypOaVZwAC4xhrG0h32Emm-Cx1F38P2AEAAwIAA3kAAzoE',
    'AgACAgIAAxkBAAEPMfVpuuGdCuGgeeOBc1e4cQthdWA3OAAC6BhrG0h32EkCxf1P9qKWzwEAAwIAA3kAAzoE',
]

V2_PHOTOS = [
    'AgACAgIAAxkBAAEPMfhpuuGu0Rg_-nKG-PcvViCGfoN4AQAC6RhrG0h32El2wcWMdvvLrgEAAwIAA3kAAzoE',
    'AgACAgIAAxkBAAEPMfppuuHSQmUSRF9AtlPh8S_vYZpICgAC6hhrG0h32Elfi-ITfrYC6QEAAwIAA3kAAzoE',
    'AgACAgIAAxkBAAEPMfxpuuHnZOPTsCVK3JKqaYR_2TzIUAAC7RhrG0h32EmvaFothcL4KAEAAwIAA3kAAzoE',
]

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


@router.callback_query(F.data == 'import')
async def import_select_os(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        text=lexicon['import_start'],
        reply_markup=keyboard_import_os()
    )


@router.callback_query(F.data.in_(OS_CALLBACKS))
async def import_select_app(callback: CallbackQuery):
    await callback.answer()
    await callback.message.answer(
        text=lexicon['import_select_app'],
        reply_markup=keyboard_import_app(callback.data)
    )


@router.callback_query(
    F.data.startswith('import_') &
    (F.data.endswith('_incy') | F.data.endswith('_happ') | F.data.endswith('_v2'))
)
async def import_select_sub(callback: CallbackQuery):
    await callback.answer()
    links = await x3.active_subscription_links(callback.from_user.id)

    if not links:
        await callback.message.answer(
            text=lexicon['no_sub'],
            reply_markup=create_kb(1, back_to_main='🔙 Назад')
        )
        return

    await callback.message.answer(
        text=lexicon['import_select_sub'],
        reply_markup=keyboard_import_sub(callback.data, links)
    )


@router.callback_query(
    F.data.regexp(r'^import_(android|ios|windows|macos)_(incy|happ|v2)_sub_(main|3|10|white)$')
)
async def import_end(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split('_')
    os_key = parts[1]
    app_key = parts[2]
    slot_key = parts[4]

    username = x3.username_for_slot(callback.from_user.id, slot_key)
    sub_url = await x3.sublink(username)
    label = next(
        (slot_label for key, _suffix, slot_label in x3.SUBSCRIPTION_SLOTS if key == slot_key),
        '💫 Ваша подписка ВПН',
    )

    if not sub_url:
        await callback.message.answer(
            '❌ Не удалось получить ссылку. Обратитесь в поддержку.',
            reply_markup=create_kb(1, back_to_main='🔙 Назад')
        )
        return

    urls = IMPORT_URLS[os_key][app_key]
    url_app = urls['url_app']

    if app_key == 'incy':
        lexicon_key = 'import_end_incy'
        photos = INCY_PHOTOS
    elif app_key == 'happ':
        lexicon_key = 'import_end_happ'
        photos = HAPP_PHOTOS
    else:
        lexicon_key = 'import_end_v2'
        photos = V2_PHOTOS

    caption = lexicon[lexicon_key].format(
        os=OS_DISPLAY[os_key],
        app=APP_DISPLAY[app_key],
        label=label,
        url_app=url_app,
        url_import=sub_url,
    )

    media = [InputMediaPhoto(media=file_id) for file_id in photos]
    media[0] = InputMediaPhoto(media=photos[0], caption=caption, parse_mode='HTML')

    await callback.message.answer_media_group(media=media)


