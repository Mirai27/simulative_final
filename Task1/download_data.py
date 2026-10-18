"""Скачать ответы CityBikes API для первого задания и сохранить их без изменений."""

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


API_URL = "https://api.citybik.es/v2"
DEFAULT_OUTPUT = Path(__file__).resolve().parent.parent / "api_snapshot"
logger = logging.getLogger(__name__)


def fetch_json(url, timeout):
    logger.info("GET %s", url)
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "citybikes-student-project/1.0"},
    )
    with urlopen(request, timeout=timeout) as response:
        body = response.read()
    # Разбор проверяет JSON. На диск записываем исходные байты ответа.
    return body, json.loads(body)


def download_data(network_ids, output_dir, timeout):
    network_ids = list(dict.fromkeys(network_ids))
    if any(not network_id or any(c not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for c in network_id)
           for network_id in network_ids):
        raise ValueError("ID сети может содержать только латинские строчные буквы, цифры, дефис и подчёркивание.")
    started = datetime.now(timezone.utc)
    directory = output_dir / started.strftime("%Y-%m-%d_%H-%M-%S_%f_UTC")
    directory.mkdir(parents=True, exist_ok=False)
    logger.info("Папка выгрузки: %s", directory)

    urls = [f"{API_URL}/networks"]
    body, data = fetch_json(urls[0], timeout)
    if not isinstance(data, dict) or not isinstance(data.get("networks"), list):
        raise ValueError("В ответе списка сетей отсутствует массив networks")
    networks = data["networks"]
    if any(not isinstance(n, dict) or not isinstance(n.get("id"), str) for n in networks):
        raise ValueError("Список сетей содержит запись без строкового id")
    unknown_ids = set(network_ids) - {n["id"] for n in networks}
    if unknown_ids:
        raise ValueError(f"В текущем API нет выбранных сетей: {', '.join(sorted(unknown_ids))}")
    (directory / "networks.json").write_bytes(body)
    logger.info("Сохранено сетей: %d", len(networks))

    for network_id in network_ids:
        url = f"{API_URL}/networks/{network_id}"
        body, data = fetch_json(url, timeout)
        network = data.get("network") if isinstance(data, dict) else None
        if not isinstance(network, dict) or network.get("id") != network_id:
            raise ValueError(f"Ответ API не соответствует сети {network_id}")
        stations = network.get("stations")
        if not isinstance(stations, list) or any(not isinstance(s, dict) for s in stations):
            raise ValueError(f"В ответе сети {network_id} отсутствует массив объектов stations")
        (directory / f"network_{network_id}.json").write_bytes(body)
        urls.append(url)
        logger.info("%s: сохранено станций %d", network_id, len(stations))

    manifest = {"downloaded_at_utc": started.isoformat(), "selected_network_ids": network_ids, "urls": urls}
    (directory / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    logger.info("Выгрузка завершена: %s", directory)
    return directory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument(
        "--networks", nargs="+", default=["divvy"], metavar="ID",
        help="Идентификаторы сетей; по умолчанию divvy",
    )
    selection.add_argument("--list-only", action="store_true", help="Скачать только список сетей")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT, help="Родительская папка выгрузок")
    parser.add_argument("--timeout", type=float, default=30, help="Тайм-аут запроса в секундах")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout должен быть положительным")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    network_ids = [] if args.list_only else args.networks
    try:
        download_data(network_ids, args.output_dir, args.timeout)
    except HTTPError as error:
        logger.error("API вернул HTTP %d: %s", error.code, error.reason)
        return 1
    except (URLError, OSError, ValueError) as error:
        logger.error("Выгрузка не завершена: %s", error)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
