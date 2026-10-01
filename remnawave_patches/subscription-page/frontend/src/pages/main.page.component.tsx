/**
 * Замена для: remnawave/subscription-page → frontend/src/pages/main.page.component.tsx
 * (или src/pages/main/ui/components/main.page.component.tsx — тот же файл по смыслу).
 *
 * API (заголовок X-Sub-Page-Api-Key = SUB_PAGE_API_KEY из .env бота):
 *   GET  /api/v1/sub_page/payment-options?username=&user_id=
 *   POST /api/v1/sub_page/pay/renew/fk_sbp|fk_card|stars|cryptobot
 *   POST /api/v1/sub_page/pay/add_devices/fk_sbp|fk_card|stars|cryptobot
 *
 * Legacy username (*_3, *_10): блоки оплаты и «Добавить устройство» не показываются.
 *
 * VITE_SUB_PAGE_PAY_API_BASE, VITE_SUB_PAGE_PAY_API_KEY — при сборке фронта.
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
    Loader,
    Modal,
    SimpleGrid,
    Stack,
    Text,
    Title,
    UnstyledButton
} from '@mantine/core'
import { IconTrash } from '@tabler/icons-react'
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
import {
    deleteSubPageDevice,
    fetchSubPageDevices,
    parseAppNameFromUserAgent,
    parseSubPageUserId,
    subPageBotApiConfig,
    SubPageDevicesInfo
} from '@shared/utils/sub-page-bot-api'

type PayMethodId = 'fk_sbp' | 'fk_card' | 'stars' | 'cryptobot'

const PAY_METHODS_ALL: ReadonlyArray<{ id: PayMethodId; label: string }> = [
    { id: 'fk_sbp', label: 'СБП' },
    { id: 'fk_card', label: 'Карты РФ' },
    { id: 'stars', label: 'Telegram Stars' },
    { id: 'cryptobot', label: 'Telegram Cryptobot' }
]
const PAY_METHODS_SITE = PAY_METHODS_ALL.filter(
    (m) => m.id === 'fk_sbp' || m.id === 'fk_card'
)

type RenewOption = {
    months: number
    price_rub: number
    devices: number
    tariff_id: string
}

type PaymentOptions = {
    legacy_slot: boolean
    devices: number | null
    subscription_active: boolean
    renew_options: RenewOption[]
    add_devices: {
        current_devices: number
        max_add: number
        billable_months: number
        options: { add_count: number; price_rub: number }[]
    } | null
}

type PayIntent =
    | { kind: 'renew'; months: number }
    | { kind: 'add_devices'; add_count: number }

/** Legacy-слоты 3/10 устройств — без оплаты на странице подписки. */
function isLegacySubscriptionUsername(username: string): boolean {
    let u = username.trim()
    if (u.endsWith('_white')) u = u.slice(0, -'_white'.length)
    return u.endsWith('_3') || u.endsWith('_10')
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
    return `${m} — ${opt.price_rub} ₽`
}

function SubscriptionBillingSection({ isMobile }: { isMobile: boolean }) {
    const { user } = useSubscription()
    const userId = useMemo(() => parseSubPageUserId(user.username), [user.username])
    const isSiteUser = userId != null && userId <= 0
    const payMethods = isSiteUser ? PAY_METHODS_SITE : PAY_METHODS_ALL
    const payCfg = useMemo(() => subPageBotApiConfig(), [])

    const [options, setOptions] = useState<PaymentOptions | null>(null)
    const [optionsError, setOptionsError] = useState<string | null>(null)
    const [loadingOptions, setLoadingOptions] = useState(false)

    const [renewExpanded, setRenewExpanded] = useState(true)
    const [addExpanded, setAddExpanded] = useState(false)
    const [modalOpen, setModalOpen] = useState(false)
    const [intent, setIntent] = useState<PayIntent | null>(null)
    const [busyMethod, setBusyMethod] = useState<PayMethodId | null>(null)
    const [errorText, setErrorText] = useState<string | null>(null)

    const legacySlot = isLegacySubscriptionUsername(user.username)

    useEffect(() => {
        if (legacySlot || userId == null || !payCfg.apiKey) return
        let cancelled = false
        const load = async () => {
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
                if (cancelled) return
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
                const parsed = data as PaymentOptions
                if (parsed.legacy_slot) {
                    setOptions(null)
                    return
                }
                setOptions(parsed)
            } catch {
                if (!cancelled) {
                    setOptionsError('Не удалось загрузить тарифы')
                    setOptions(null)
                }
            } finally {
                if (!cancelled) setLoadingOptions(false)
            }
        }
        void load()
        return () => {
            cancelled = true
        }
    }, [legacySlot, payCfg.apiBase, payCfg.apiKey, user.username, userId])

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
            const segment = intent.kind === 'renew' ? 'renew' : 'add_devices'
            const path = `/api/v1/sub_page/pay/${segment}/${method}`
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
                const obj = data as { payment_url?: string; bot_url?: string }
                const redirect = obj.payment_url || obj.bot_url
                if (redirect) {
                    window.location.assign(redirect)
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

    if (legacySlot || !payCfg.apiKey || userId == null) {
        return null
    }

    if (loadingOptions && !options) {
        return (
            <Card p="md" radius="lg" withBorder>
                <Center py="sm">
                    <Loader size="sm" />
                </Center>
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

    if (!options) {
        return null
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
                    <UnstyledButton onClick={() => setRenewExpanded((v) => !v)} w="100%">
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
                                    transform: renewExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                                    transition: 'transform 200ms ease'
                                }}
                            >
                                ▼
                            </Box>
                        </Group>
                    </UnstyledButton>
                    <Collapse expanded={renewExpanded}>
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
                            Сейчас {devicesLabel(options.add_devices!.current_devices)}. Доплата до
                            конца подписки (~{options.add_devices!.billable_months} мес.).
                        </Text>
                        <Collapse expanded={addExpanded}>
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
                        {payMethods.map((m) => (
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

function SubscriptionDevicesBlock({ isMobile: _isMobile }: { isMobile: boolean }) {
    const { user } = useSubscription()
    const botApiCfg = useMemo(() => subPageBotApiConfig(), [])
    const [devicesExpanded, setDevicesExpanded] = useState(false)
    const [devicesInfo, setDevicesInfo] = useState<SubPageDevicesInfo | null>(null)
    const [loading, setLoading] = useState(false)
    const [deletingHwid, setDeletingHwid] = useState<string | null>(null)

    const loadDevices = useCallback(async () => {
        setLoading(true)
        const info = await fetchSubPageDevices(botApiCfg, user.username)
        setDevicesInfo(info)
        setLoading(false)
    }, [botApiCfg, user.username])

    const toggleDevices = useCallback(() => {
        setDevicesExpanded((v) => {
            const next = !v
            if (next && devicesInfo == null && !loading) {
                void loadDevices()
            }
            return next
        })
    }, [devicesInfo, loading, loadDevices])

    const handleDelete = useCallback(
        async (hwid: string) => {
            setDeletingHwid(hwid)
            const ok = await deleteSubPageDevice(botApiCfg, user.username, hwid)
            if (ok) {
                setDevicesInfo((prev) =>
                    prev
                        ? {
                              ...prev,
                              devices: prev.devices.filter((d) => d.hwid !== hwid),
                              total: prev.total - 1
                          }
                        : prev
                )
            }
            setDeletingHwid(null)
        },
        [botApiCfg, user.username]
    )

    if (!botApiCfg.apiKey) {
        return null
    }

    return (
        <Card p="md" radius="lg" withBorder>
            <Stack gap="md">
                <UnstyledButton onClick={toggleDevices} w="100%">
                    <Group gap="sm" justify="space-between" wrap="nowrap">
                        <Title c="white" order={5} style={{ flex: 1, textAlign: 'left' }}>
                            Устройства
                            {devicesInfo
                                ? ` (${devicesInfo.total}${devicesInfo.limit ? `/${devicesInfo.limit}` : ''})`
                                : ''}
                        </Title>
                        <Box
                            aria-hidden
                            c="dimmed"
                            style={{
                                flexShrink: 0,
                                fontSize: 12,
                                lineHeight: 1,
                                transform: devicesExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                                transition: 'transform 200ms ease'
                            }}
                        >
                            ▼
                        </Box>
                    </Group>
                </UnstyledButton>
                <Collapse expanded={devicesExpanded}>
                    {loading && (
                        <Center py="md">
                            <Loader size="sm" />
                        </Center>
                    )}
                    {!loading && devicesInfo && devicesInfo.devices.length === 0 && (
                        <Text c="dimmed" size="sm">
                            Нет подключённых устройств
                        </Text>
                    )}
                    {!loading && devicesInfo && devicesInfo.devices.length > 0 && (
                        <Stack gap="xs">
                            {devicesInfo.devices.map((d) => (
                                <Group justify="space-between" key={d.hwid} wrap="nowrap">
                                    <Stack gap={0} style={{ minWidth: 0, flex: 1 }}>
                                        <Text c="white" fw={500} size="sm" truncate>
                                            {d.deviceModel || d.platform || 'Неизвестное устройство'}
                                        </Text>
                                        <Text c="dimmed" size="xs" truncate>
                                            {[d.platform, d.osVersion, parseAppNameFromUserAgent(d.userAgent)]
                                                .filter(Boolean)
                                                .join(' · ') || d.hwid}
                                        </Text>
                                    </Stack>
                                    <Button
                                        color="red"
                                        loading={deletingHwid === d.hwid}
                                        onClick={() => void handleDelete(d.hwid)}
                                        radius="md"
                                        size="xs"
                                        variant="light"
                                    >
                                        <IconTrash size={14} />
                                    </Button>
                                </Group>
                            ))}
                        </Stack>
                    )}
                </Collapse>
            </Stack>
        </Card>
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

                    <SubscriptionBillingSection isMobile={isMobile} />

                    <SubscriptionDevicesBlock isMobile={isMobile} />

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