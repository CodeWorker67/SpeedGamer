"""Разбор payload платежей (key:value и флаги без значения, напр. discount)."""


def parse_payment_payload(payload: str) -> dict[str, str]:
    """
    Парсит строку вида user_id:1,duration:30,...,discount[,discount].
    Элементы без «:» трактуются как флаги со значением «1» (повторы перезаписывают то же значение).
    """
    payload_parts: dict[str, str] = {}
    for item in str(payload).split(","):
        item = item.strip()
        if not item:
            continue
        if ":" in item:
            k, v = item.split(":", 1)
            payload_parts[k.strip()] = v.strip()
        else:
            payload_parts[item] = "1"
    return payload_parts
