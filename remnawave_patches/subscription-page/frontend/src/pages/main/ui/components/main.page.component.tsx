/**
 * Замена для: remnawave/subscription-page → frontend/src/pages/main/ui/components/main.page.component.tsx
 *
 * URL и ключ берутся из переменных Vite (префикс VITE_) — их задают при СБОРКЕ фронта.
 *
 * 1) В /opt/remnawave/.env.sub добавьте (без кавычек, без пробелов вокруг =):
 *    VITE_SUB_PAGE_PAY_API_BASE=http://btg.speedgamer.top
 *    VITE_SUB_PAGE_PAY_API_KEY=<тот же SUB_PAGE_API_KEY, что в .env бота>
 *
 *    Если VITE_SUB_PAGE_PAY_API_BASE не задан, по умолчанию используется http://btg.speedgamer.top
 *
 * 2) Сборка frontend (подставьте путь к клону subscription-page):
 *    docker run --rm -it --env-file /opt/remnawave/.env.sub \
 *      -v /opt/subscription-page/frontend:/work -w /work \
 *      -e NODE_OPTIONS=--max-old-space-size=4096 \
 *      node:24-bookworm-slim bash -lc "npm ci && npm run start:build"
 *
 * 3) docker build образа subscription-page и compose up, как раньше.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import {
    Box,
    Button,
    Card,
    Center,
    Collapse,
    Container,
    Group,
    Image,
    Modal,
    SimpleGrid,
    Stack,
    Text,
    Title,
    UnstyledButton
} from '@mantine/core'
import { TSubscriptionPagePlatformKey } from '@remnawave/subscription-page-types'

import {
    AccordionBlockRenderer,
    CardsBlockRenderer,
    InstallationGuideConnector,
    MinimalBlockRenderer,
    RawKeysWidget,
    SubscriptionInfoCardsWidget,
    SubscriptionInfoCollapsedWidget,
    SubscriptionInfoExpandedWidget,
    SubscriptionLinkWidget,
    TimelineBlockRenderer
} from '@widgets/main'
import { useAppConfig, useAppConfigStoreActions, useCurrentLang } from '@entities/app-config-store'
import { useSubscription } from '@entities/subscription-info-store'
import { LanguagePicker } from '@shared/ui/language-picker/language-picker.shared'
import { Page, RemnawaveLogo } from '@shared/ui'

const DEFAULT_PAY_API_BASE = 'http://btg.speedgamer.top'

function subPagePayFromBuild(): { apiBase: string; apiKey: string } {
    return {
        apiBase: String(import.meta.env.VITE_SUB_PAGE_PAY_API_BASE ?? DEFAULT_PAY_API_BASE).trim(),
        apiKey: String(import.meta.env.VITE_SUB_PAGE_PAY_API_KEY ?? '').trim()
    }
}

type PayMethodId = 'fk_sbp' | 'fk_card'

const PAY_METHODS: ReadonlyArray<{ id: PayMethodId; label: string }> = [
    { id: 'fk_sbp', label: 'СБП' },
    { id: 'fk_card', label: 'Карты РФ' }
]

type RenewOption = {
    months: number
    price_rub: number
    devices: number
    tariff_id: string
}

type AddDeviceOption = {
    add_count: number
    price_rub: number
}

type PaymentOptions = {
    legacy_slot: boolean
    payment_allowed: boolean
    message?: string
    devices: number | null
    subscription_active: boolean
    main_subscription_active: boolean
    renew_options: RenewOption[]
    add_devices: {
        current_devices: number
        max_add: number
        billable_months: number
        options: AddDeviceOption[]
    } | null
}

type PayIntent =
    | { kind: 'renew'; months: number }
    | { kind: 'add_devices'; add_count: number }

/**
 * user_id из username страницы подписки.
 * Telegram: 123456789. Сайт: -1833 или n-2 (короткие отрицательные id).
 * Снимаются суффиксы _white, затем _10, затем _3.
 */
function parseSubPageUserId(username: string): number | null {
    let base = username.trim()
    if (base.endsWith('_white')) base = base.slice(0, -'_white'.length)
    if (base.endsWith('_10')) base = base.slice(0, -'_10'.length)
    if (base.endsWith('_3')) base = base.slice(0, -'_3'.length)
    const numeric = (s: string): number | null => {
        if (!/^-?\d+$/.test(s)) return null
        const n = Number.parseInt(s, 10)
        return Number.isFinite(n) ? n : null
    }
    const direct = numeric(base)
    if (direct != null) return direct
    if (base.startsWith('n')) return numeric(base.slice(1))
    return null
}

function devicesLabel(n: number): string {
    const mod10 = n % 10
    const mod100 = n % 100
    if (mod100 >= 11 && mod100 <= 14) return `${n} устройств`
    if (mod10 === 1) return `${n} устройство`
    if (mod10 >= 2 && mod10 <= 4) return `${n} устройства`
    return `${n} устройств`
}

function renewLabel(opt: RenewOption): string {
    const m = opt.months === 1 ? '1 месяц' : `${opt.months} месяца`
    return `${m} — ${opt.price_rub} ₽ (${devicesLabel(opt.devices)})`
}

function SubscriptionPayBlock({ isMobile }: { isMobile: boolean }) {
    const { user } = useSubscription()
    const userId = useMemo(() => parseSubPageUserId(user.username), [user.username])
    const payCfg = useMemo(() => subPagePayFromBuild(), [])

    const [options, setOptions] = useState<PaymentOptions | null>(null)
    const [optionsError, setOptionsError] = useState<string | null>(null)
    const [loadingOptions, setLoadingOptions] = useState(false)

    const [payExpanded, setPayExpanded] = useState(true)
    const [addExpanded, setAddExpanded] = useState(false)
    const [modalOpen, setModalOpen] = useState(false)
    const [intent, setIntent] = useState<PayIntent | null>(null)
    const [busyMethod, setBusyMethod] = useState<PayMethodId | null>(null)
    const [errorText, setErrorText] = useState<string | null>(null)

    const loadOptions = useCallback(async () => {
        if (userId == null || !payCfg.apiKey) return
        setLoadingOptions(true)
        setOptionsError(null)
        const qs = new URLSearchParams({
            username: user.username,
            user_id: String(userId)
        })
        const url = `${payCfg.apiBase.replace(/\/$/, '')}/api/v1/sub_page/payment-options?${qs}`
        try {
            const res = await fetch(url, {
                headers: { 'X-Sub-Page-Api-Key': payCfg.apiKey }
            })
            const data: unknown = await res.json().catch(() => ({}))
            if (!res.ok) {
                const msg =
                    typeof data === 'object' &&
                    data !== null &&
                    'detail' in data &&
                    typeof (data as { detail?: unknown }).detail === 'string'
                        ? (data as { detail: string }).detail
                        : `Ошибка ${res.status}`
                setOptionsError(msg)
                setOptions(null)
                return
            }
            setOptions(data as PaymentOptions)
        } catch {
            setOptionsError('Не удалось загрузить тарифы')
            setOptions(null)
        } finally {
            setLoadingOptions(false)
        }
    }, [payCfg.apiBase, payCfg.apiKey, user.username, userId])

    useEffect(() => {
        void loadOptions()
    }, [loadOptions])

    const openPay = useCallback((next: PayIntent) => {
        setErrorText(null)
        setIntent(next)
        setModalOpen(true)
    }, [])

    const closeModal = useCallback(() => {
        if (busyMethod) return
        setModalOpen(false)
        setIntent(null)
        setErrorText(null)
    }, [busyMethod])

    const submitPay = useCallback(
        async (method: PayMethodId) => {
            if (userId == null || intent == null || !payCfg.apiKey) return
            setBusyMethod(method)
            setErrorText(null)
            const base = payCfg.apiBase.replace(/\/$/, '')
            const path =
                intent.kind === 'renew'
                    ? `/api/v1/sub_page/pay/renew/${method}`
                    : `/api/v1/sub_page/pay/add_devices/${method}`
            const body =
                intent.kind === 'renew'
                    ? { user_id: userId, username: user.username, months: intent.months }
                    : {
                          user_id: userId,
                          username: user.username,
                          add_count: intent.add_count
                      }
            try {
                const res = await fetch(`${base}${path}`, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'X-Sub-Page-Api-Key': payCfg.apiKey
                    },
                    body: JSON.stringify(body)
                })
                const data: unknown = await res.json().catch(() => ({}))
                if (!res.ok) {
                    const msg =
                        typeof data === 'object' &&
                        data !== null &&
                        'detail' in data &&
                        typeof (data as { detail?: unknown }).detail === 'string'
                            ? (data as { detail: string }).detail
                            : `Ошибка ${res.status}`
                    setErrorText(msg)
                    return
                }
                const obj = data as { payment_url?: string }
                if (obj.payment_url && typeof obj.payment_url === 'string') {
                    window.location.assign(obj.payment_url)
                    return
                }
                setErrorText('В ответе нет ссылки для перехода')
            } catch {
                setErrorText('Сеть недоступна или сервер не ответил')
            } finally {
                setBusyMethod(null)
            }
        },
        [intent, payCfg.apiBase, payCfg.apiKey, user.username, userId]
    )

    if (!payCfg.apiKey) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    Оплата: не задан VITE_SUB_PAGE_PAY_API_KEY при сборке фронта. Добавьте его в .env.sub и
                    пересоберите образ (см. комментарий в начале main.page.component.tsx).
                </Text>
            </Card>
        )
    }

    if (userId == null) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    Оплата: не удалось определить user_id из имени пользователя подписки.
                </Text>
            </Card>
        )
    }

    if (loadingOptions && !options) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    Загрузка тарифов…
                </Text>
            </Card>
        )
    }

    if (optionsError) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="red" size="sm">
                    {optionsError}
                </Text>
            </Card>
        )
    }

    if (!options) return null

    if (options.legacy_slot) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Text c="dimmed" size="sm">
                    {options.message ||
                        'Эта подписка (3 или 10 устройств, старый формат) не продлевается здесь. Оформите новую в боте или на сайте.'}
                </Text>
            </Card>
        )
    }

    const devices = options.devices ?? 5
    const showAdd =
        options.add_devices != null &&
        options.add_devices.max_add > 0 &&
        options.add_devices.options.length > 0

    return (
        <>
            <Card p="md" radius="lg" withBorder>
                <Stack gap="md">
                    <UnstyledButton onClick={() => setPayExpanded((v) => !v)} w="100%">
                        <Group gap="sm" justify="space-between" wrap="nowrap">
                            <Title c="white" order={5} style={{ flex: 1, textAlign: 'left' }}>
                                Продление подписки · {devicesLabel(devices)}
                            </Title>
                            <Box
                                aria-hidden
                                c="dimmed"
                                style={{
                                    flexShrink: 0,
                                    fontSize: 12,
                                    lineHeight: 1,
                                    transform: payExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                                    transition: 'transform 200ms ease'
                                }}
                            >
                                ▼
                            </Box>
                        </Group>
                    </UnstyledButton>
                    <Collapse in={payExpanded}>
                        <Stack gap="sm">
                            {options.renew_options.map((row) => (
                                <Button
                                    key={row.tariff_id}
                                    fullWidth
                                    justify="space-between"
                                    onClick={() => openPay({ kind: 'renew', months: row.months })}
                                    radius="md"
                                    size={isMobile ? 'sm' : 'md'}
                                    variant="light"
                                >
                                    <Text fw={500} size="sm" style={{ textAlign: 'left' }}>
                                        {renewLabel(row)}
                                    </Text>
                                </Button>
                            ))}
                        </Stack>
                    </Collapse>
                </Stack>
            </Card>

            {showAdd ? (
                <Card p="md" radius="lg" withBorder>
                    <Stack gap="md">
                        <UnstyledButton onClick={() => setAddExpanded((v) => !v)} w="100%">
                            <Group gap="sm" justify="space-between" wrap="nowrap">
                                <Title c="white" order={5} style={{ flex: 1, textAlign: 'left' }}>
                                    Добавить устройство
                                </Title>
                                <Box
                                    aria-hidden
                                    c="dimmed"
                                    style={{
                                        flexShrink: 0,
                                        fontSize: 12,
                                        lineHeight: 1,
                                        transform: addExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                                        transition: 'transform 200ms ease'
                                    }}
                                >
                                    ▼
                                </Box>
                            </Group>
                        </UnstyledButton>
                        <Text c="dimmed" size="xs">
                            Сейчас {devicesLabel(options.add_devices!.current_devices)}. Доплата до конца
                            подписки (~{options.add_devices!.billable_months} мес.).
                        </Text>
                        <Collapse in={addExpanded}>
                            <Stack gap="sm">
                                {options.add_devices!.options.map((row) => (
                                    <Button
                                        key={row.add_count}
                                        fullWidth
                                        onClick={() =>
                                            openPay({ kind: 'add_devices', add_count: row.add_count })
                                        }
                                        radius="md"
                                        size={isMobile ? 'sm' : 'md'}
                                        variant="light"
                                    >
                                        <Text fw={500} size="sm">
                                            + {row.add_count} уст. — {row.price_rub} ₽
                                        </Text>
                                    </Button>
                                ))}
                            </Stack>
                        </Collapse>
                    </Stack>
                </Card>
            ) : null}

            <Modal
                centered
                onClose={closeModal}
                opened={modalOpen}
                radius="lg"
                title="Выберите способ оплаты"
            >
                <Stack gap="sm">
                    {errorText ? (
                        <Text c="red" size="sm">
                            {errorText}
                        </Text>
                    ) : null}
                    <SimpleGrid cols={1} spacing="xs">
                        {PAY_METHODS.map((m) => (
                            <Button
                                key={m.id}
                                loading={busyMethod === m.id}
                                onClick={() => void submitPay(m.id)}
                                radius="md"
                                variant="filled"
                            >
                                {m.label}
                            </Button>
                        ))}
                    </SimpleGrid>
                    <Button disabled={!!busyMethod} onClick={closeModal} variant="subtle">
                        Отмена
                    </Button>
                </Stack>
            </Modal>
        </>
    )
}

interface IMainPageComponentProps {
    isMobile: boolean
    platform: TSubscriptionPagePlatformKey | undefined
}

const BLOCK_RENDERERS = {
    cards: CardsBlockRenderer,
    timeline: TimelineBlockRenderer,
    accordion: AccordionBlockRenderer,
    minimal: MinimalBlockRenderer
} as const

const SUBSCRIPTION_INFO_BLOCK_RENDERERS = {
    cards: SubscriptionInfoCardsWidget,
    collapsed: SubscriptionInfoCollapsedWidget,
    expanded: SubscriptionInfoExpandedWidget,
    hidden: null
} as const

export const MainPageComponent = ({ isMobile, platform }: IMainPageComponentProps) => {
    const config = useAppConfig()
    const currentLang = useCurrentLang()
    const { setLanguage } = useAppConfigStoreActions()

    const brandName = config.brandingSettings.title
    let hasCustomLogo = !!config.brandingSettings.logoUrl

    if (hasCustomLogo) {
        if (config.brandingSettings.logoUrl.includes('docs.rw')) {
            hasCustomLogo = false
        }
    }

    const hasPlatformApps: Record<TSubscriptionPagePlatformKey, boolean> = {
        ios: Boolean(config.platforms.ios?.apps.length),
        android: Boolean(config.platforms.android?.apps.length),
        linux: Boolean(config.platforms.linux?.apps.length),
        macos: Boolean(config.platforms.macos?.apps.length),
        windows: Boolean(config.platforms.windows?.apps.length),
        androidTV: Boolean(config.platforms.androidTV?.apps.length),
        appleTV: Boolean(config.platforms.appleTV?.apps.length)
    }

    const atLeastOnePlatformApp = Object.values(hasPlatformApps).some((value) => value)

    const SubscriptionInfoBlockRenderer =
        SUBSCRIPTION_INFO_BLOCK_RENDERERS[config.uiConfig.subscriptionInfoBlockType]

    return (
        <Page>
            <Box className="header-wrapper" py="md">
                <Container maw={1200} px={{ base: 'md', sm: 'lg', md: 'xl' }}>
                    <Group justify="space-between">
                        <Group gap="sm" style={{ userSelect: 'none' }} wrap="nowrap">
                            {hasCustomLogo ? (
                                <Image
                                    alt="logo"
                                    fit="contain"
                                    src={config.brandingSettings.logoUrl}
                                    style={{
                                        width: '32px',
                                        height: '32px',
                                        flexShrink: 0
                                    }}
                                />
                            ) : (
                                <RemnawaveLogo c="cyan" size={32} />
                            )}
                            <Title
                                c={hasCustomLogo ? 'white' : 'cyan'}
                                fw={700}
                                order={4}
                                size="lg"
                            >
                                {brandName}
                            </Title>
                        </Group>

                        <SubscriptionLinkWidget
                            hideGetLink={config.baseSettings.hideGetLinkButton}
                            supportUrl={config.brandingSettings.supportUrl}
                        />
                    </Group>
                </Container>
            </Box>

            <Container
                maw={1200}
                px={{ base: 'md', sm: 'lg', md: 'xl' }}
                py="xl"
                style={{ position: 'relative', zIndex: 1 }}
            >
                <Stack gap="xl">
                    {SubscriptionInfoBlockRenderer && (
                        <SubscriptionInfoBlockRenderer isMobile={isMobile} />
                    )}

                    <SubscriptionPayBlock isMobile={isMobile} />

                    {atLeastOnePlatformApp && (
                        <InstallationGuideConnector
                            BlockRenderer={
                                BLOCK_RENDERERS[config.uiConfig.installationGuidesBlockType]
                            }
                            hasPlatformApps={hasPlatformApps}
                            isMobile={isMobile}
                            platform={platform}
                        />
                    )}

                    <RawKeysWidget isMobile={isMobile} />

                    <Center>
                        <LanguagePicker
                            currentLang={currentLang}
                            locales={config.locales}
                            onLanguageChange={setLanguage}
                        />
                    </Center>
                </Stack>
            </Container>
        </Page>
    )
}
