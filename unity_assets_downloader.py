# by Ulysses, wdwxy12345@gmail.com
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from email import message_from_binary_file, policy
import html
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_CONFIG = {
    "bearer_token": "",
    "cookie_file": "cookie.txt",
    "purchase_api": "https://packages-v2.unity.cn/-/api/purchases",
    "download_api_template": "https://assetstore.unity.com/api/downloads/{package_id}",
    "download_dir": "downloads",
    "limit": 100,
    "order_by": "name",
    "order": "asc",
    "request_timeout_sec": 60,
    "max_workers": 3,
    "download_retries": 2,
    "purchases_export_file": "purchases_snapshot.json",
}


def create_config_if_missing(config_path: Path) -> None:
    if config_path.exists():
        return
    config_path.write_text(
        json.dumps(DEFAULT_CONFIG, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] 已创建配置文件: {config_path}")


def load_config(config_path: Path) -> Dict:
    create_config_if_missing(config_path)
    raw = config_path.read_text(encoding="utf-8")
    data = json.loads(raw)
    merged = DEFAULT_CONFIG.copy()
    merged.update(data)
    return merged


def load_cookie_from_file(root: Path, cookie_file: str) -> str:
    cookie_path = (root / cookie_file).resolve()
    if not cookie_path.exists():
        raise FileNotFoundError(f"cookie 文件不存在: {cookie_path}")
    return cookie_path.read_text(encoding="utf-8").strip()


def sanitize_filename(name: str) -> str:
    sanitized = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip()
    sanitized = sanitized.rstrip(". ")
    return sanitized or "unnamed_asset"


def normalize_asset_name_for_match(name: str) -> str:
    value = html.unescape(name or "")
    value = value.lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[’'`]", "", value)
    value = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", value)
    return value


def build_asset_name_variants(name: str) -> Set[str]:
    raw = html.unescape(name or "").strip()
    if not raw:
        return set()

    variants: Set[str] = set()
    variants.add(normalize_asset_name_for_match(raw))

    without_brackets = re.sub(r"\([^)]*\)|（[^）]*）", " ", raw)
    variants.add(normalize_asset_name_for_match(without_brackets))

    without_long_suffix = re.sub(
        r"\s*\|\s*updates?\s+in\s+new\s+charactercontroller\s+package\s*$",
        "",
        raw,
        flags=re.IGNORECASE,
    )
    variants.add(normalize_asset_name_for_match(without_long_suffix))

    return {v for v in variants if v}


def tokenize_asset_name(name: str) -> Set[str]:
    value = html.unescape(name or "").lower()
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]+", value)
    return {token for token in tokens if token}


def extract_text_bodies_from_eml(eml_path: Path) -> Tuple[str, str]:
    if not eml_path.exists():
        raise FileNotFoundError(f"EML 文件不存在: {eml_path}")

    with eml_path.open("rb") as f:
        message = message_from_binary_file(f, policy=policy.default)

    html_parts: List[str] = []
    plain_parts: List[str] = []

    def decode_part(part) -> str:
        try:
            content = part.get_content()
            if isinstance(content, str):
                return content
        except Exception:
            pass

        payload = part.get_payload(decode=True) or b""
        charset = part.get_content_charset() or "utf-8"
        try:
            return payload.decode(charset, errors="replace")
        except Exception:
            return payload.decode("utf-8", errors="replace")

    if message.is_multipart():
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            content_type = (part.get_content_type() or "").lower()
            content = decode_part(part)
            if not content:
                continue
            if content_type == "text/html":
                html_parts.append(content)
            elif content_type == "text/plain":
                plain_parts.append(content)
    else:
        content_type = (message.get_content_type() or "").lower()
        content = decode_part(message)
        if content_type == "text/html":
            html_parts.append(content)
        elif content_type == "text/plain":
            plain_parts.append(content)

    return "\n".join(html_parts), "\n".join(plain_parts)


def extract_updated_asset_names_from_eml(eml_path: Path) -> List[str]:
    html_body, plain_body = extract_text_bodies_from_eml(eml_path)
    html_pattern = re.compile(
        r"just\s+updated\s*<a\b[^>]*>(.*?)</a>\s*to\s+version",
        flags=re.IGNORECASE | re.DOTALL,
    )
    plain_pattern = re.compile(
        r"just\s+updated\s+(.*?)\s+to\s+version", flags=re.IGNORECASE
    )

    extracted_names: List[str] = []
    if html_body:
        for raw_name in html_pattern.findall(html_body):
            stripped = re.sub(r"<[^>]+>", "", raw_name)
            normalized = " ".join(html.unescape(stripped).split())
            if normalized:
                extracted_names.append(normalized)

    if not extracted_names and plain_body:
        for raw_name in plain_pattern.findall(plain_body):
            normalized = " ".join(html.unescape(raw_name).split())
            if normalized:
                extracted_names.append(normalized)

    if not extracted_names:
        fallback_text = eml_path.read_text(encoding="utf-8", errors="ignore")
        for raw_name in html_pattern.findall(fallback_text):
            stripped = re.sub(r"<[^>]+>", "", raw_name)
            normalized = " ".join(html.unescape(stripped).split())
            if normalized:
                extracted_names.append(normalized)

    deduped_names: List[str] = []
    seen_keys: Set[str] = set()
    for name in extracted_names:
        key = normalize_asset_name_for_match(name)
        if not key or key in seen_keys:
            continue
        deduped_names.append(name)
        seen_keys.add(key)
    return deduped_names


def select_best_purchase_match(update_name: str, purchases: List[Dict]) -> Optional[Dict]:
    update_variants = build_asset_name_variants(update_name)
    if not update_variants:
        return None

    update_tokens = tokenize_asset_name(update_name)
    best_item: Optional[Dict] = None
    best_score = 0.0

    for item in purchases:
        display_name = str(item.get("displayName") or "").strip()
        if not display_name:
            continue
        purchase_variants = build_asset_name_variants(display_name)
        if not purchase_variants:
            continue

        if update_variants & purchase_variants:
            return item

        containment_score = 0.0
        ratio_score = 0.0
        for left in update_variants:
            for right in purchase_variants:
                if left and right and (left in right or right in left):
                    containment_score = max(
                        containment_score, min(len(left), len(right)) / max(len(left), len(right))
                    )
                ratio_score = max(ratio_score, SequenceMatcher(None, left, right).ratio())

        purchase_tokens = tokenize_asset_name(display_name)
        token_overlap = 0.0
        if update_tokens:
            token_overlap = len(update_tokens & purchase_tokens) / len(update_tokens)

        combined_score = max(containment_score, ratio_score * 0.75 + token_overlap * 0.25)
        if combined_score > best_score:
            best_score = combined_score
            best_item = item

    if best_item is not None and best_score >= 0.78:
        return best_item
    return None


def filter_purchases_for_updates(
    purchases: List[Dict], updated_asset_names: List[str]
) -> Tuple[List[Dict], List[str]]:
    matched: List[Dict] = []
    unmatched: List[str] = []
    seen_package_ids: Set[int] = set()

    for update_name in updated_asset_names:
        matched_item = select_best_purchase_match(update_name, purchases)
        if not matched_item:
            unmatched.append(update_name)
            continue

        package_id = matched_item.get("packageId")
        try:
            package_id_int = int(package_id)
        except Exception:
            unmatched.append(update_name)
            continue

        if package_id_int in seen_package_ids:
            continue

        matched.append(matched_item)
        seen_package_ids.add(package_id_int)

    return matched, unmatched


def fetch_all_purchases(
    session: requests.Session,
    purchase_api: str,
    limit: int,
    order_by: str,
    order: str,
    timeout: int,
) -> List[Dict]:
    all_items: List[Dict] = []
    offset = 0
    total: Optional[int] = None

    while True:
        params = {
            "offset": offset,
            "limit": limit,
            "orderBy": order_by,
            "order": order,
        }
        resp = session.get(purchase_api, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()

        items = payload.get("results", []) or []
        if total is None:
            total = payload.get("total")

        all_items.extend(items)
        print(
            f"[INFO] 已获取 {len(all_items)} 条购买记录"
            + (f" / total={total}" if total is not None else "")
        )

        if not items:
            break
        if total is not None and len(all_items) >= total:
            break

        offset += len(items)

    return all_items


def resolve_download_response(
    session: requests.Session,
    first_response: requests.Response,
    timeout: int,
) -> requests.Response:
    content_type = (first_response.headers.get("Content-Type") or "").lower()
    if "application/json" not in content_type:
        return first_response

    try:
        body = first_response.json()
    except Exception:
        return first_response

    if not isinstance(body, dict):
        return first_response

    for key in ("url", "downloadUrl", "download_url"):
        direct_url = body.get(key)
        if isinstance(direct_url, str) and direct_url:
            return session.get(direct_url, stream=True, timeout=timeout)

    return first_response


def detect_unexpected_download_response(
    response: requests.Response,
    first_chunk: bytes,
) -> Optional[str]:
    cookie_hint = "请检查并更新 cookie.txt（可能已过期或无效）"
    content_type = (response.headers.get("Content-Type") or "").lower()
    text_like_content_type_hints = (
        "text/",
        "application/json",
        "application/xml",
        "application/xhtml+xml",
        "application/javascript",
    )
    if any(hint in content_type for hint in text_like_content_type_hints):
        return (
            f"返回了文本内容 (Content-Type={content_type or 'unknown'})，"
            f"{cookie_hint}"
        )

    if not first_chunk:
        content_length = (response.headers.get("Content-Length") or "").strip()
        if content_length == "0":
            return "响应体为空，未返回 unitypackage 数据"
        return None

    probe_text = first_chunk[:4096].decode("utf-8", errors="ignore").strip().lower()
    if probe_text:
        if probe_text.startswith("<!doctype html") or probe_text.startswith("<html"):
            return f"响应体看起来是 HTML 页面，{cookie_hint}"
        if "<html" in probe_text[:512] and "</html>" in probe_text:
            return f"响应体看起来是 HTML 页面，{cookie_hint}"

    return None


def is_chunked_stream_error(exc: Exception) -> bool:
    text = repr(exc)
    hints = (
        "InvalidChunkLength",
        "ChunkedEncodingError",
        "IncompleteRead",
        "ProtocolError",
        "Connection broken",
    )
    return any(hint in text for hint in hints)


def download_one_asset(
    session: requests.Session,
    download_api_template: str,
    package_id: int,
    output_file: Path,
    timeout: int,
    retries: int = 3,
) -> None:
    url = download_api_template.format(package_id=package_id)
    last_err: Optional[Exception] = None
    download_chunk_size = 2 * 1024 * 1024
    tmp_file = output_file.with_suffix(output_file.suffix + ".part")

    for attempt in range(1, retries + 1):
        resp: Optional[requests.Response] = None
        try:
            if tmp_file.exists():
                tmp_file.unlink()

            resp = session.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
            resp = resolve_download_response(session, resp, timeout)
            resp.raise_for_status()

            chunk_iter = resp.iter_content(chunk_size=download_chunk_size)
            first_chunk = b""
            for chunk in chunk_iter:
                if chunk:
                    first_chunk = chunk
                    break

            unexpected_reason = detect_unexpected_download_response(resp, first_chunk)
            if unexpected_reason:
                raise RuntimeError(
                    f"下载接口返回异常内容: packageId={package_id}, {unexpected_reason}"
                )

            with tmp_file.open("wb") as f:
                if first_chunk:
                    f.write(first_chunk)
                for chunk in chunk_iter:
                    if chunk:
                        f.write(chunk)
            tmp_file.replace(output_file)
            return
        except Exception as exc:
            last_err = exc
            if tmp_file.exists():
                tmp_file.unlink()
            if attempt < retries:
                chunked_error = is_chunked_stream_error(exc)
                wait_sec = attempt * 4 if chunked_error else attempt * 2
                extra_hint = (
                    " [网络分块流异常，已等待并重试]"
                    if chunked_error
                    else ""
                )
                print(
                    f"[WARN] 下载失败 packageId={package_id},"
                    f" 第 {attempt} 次重试后等待 {wait_sec}s: {exc}{extra_hint}"
                )
                time.sleep(wait_sec)
            else:
                break
        finally:
            if resp is not None:
                resp.close()

    raise RuntimeError(f"下载失败 packageId={package_id}: {last_err}")


def build_purchase_session(bearer: str, cookie: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "UnityEditor/2022.3.62f1c1 (Windows; U; Windows NT 10.0; zh)",
            "Accept": "*/*",
            "Authorization": f"Bearer {bearer}",
            "Cookie": cookie,
        }
    )
    return session


def build_download_session(cookie: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "*/*",
            "Accept-Encoding": "identity",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Referer": "https://assetstore.unity.com/",
            "Origin": "https://assetstore.unity.com",
            "Connection": "close",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
            ),
            "Cookie": cookie,
        }
    )
    retry_strategy = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=1,
        pool_maxsize=1,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def load_retry_targets(failed_log_path: Path) -> List[Dict]:
    if not failed_log_path.exists():
        raise FileNotFoundError(f"失败清单不存在: {failed_log_path}")
    payload = json.loads(failed_log_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"失败清单格式错误，期望 list: {failed_log_path}")
    return payload


def write_retry_targets(failed_log_path: Path, items: List[Dict]) -> None:
    failed_log_path.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="下载 Unity Asset Store 已购买资产（.unitypackage）"
    )
    parser.add_argument(
        "--config",
        default="asset_store_config.json",
        help="配置文件路径，默认: asset_store_config.json",
    )
    parser.add_argument(
        "--download-dir",
        default=None,
        help="覆盖配置中的下载目录",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="只重试失败清单里的条目（来自 failed_downloads.json）",
    )
    parser.add_argument(
        "--failed-log",
        default="failed_downloads.json",
        help="失败清单文件路径，默认: failed_downloads.json",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="下载并发数，默认读取配置 max_workers（默认3）",
    )
    parser.add_argument(
        "--purchases-export-file",
        default=None,
        help="素材列表导出文件路径，默认读取配置 purchases_export_file",
    )
    parser.add_argument(
        "--updates-eml-file",
        default=None,
        help="从通知邮件 .eml 提取更新素材并写入失败清单队列前部（不下载）",
    )
    parser.add_argument(
        "--updates-snapshot-file",
        default=None,
        help="更新模式使用的素材快照路径，默认读取 purchases_export_file",
    )
    parser.add_argument(
        "--force-redownload-existing",
        action="store_true",
        help="即使文件已存在也强制重下（常用于 --retry-failed）",
    )
    args = parser.parse_args()

    root = Path.cwd()
    config_path = (root / args.config).resolve()
    config = load_config(config_path)

    updates_mode_enabled = bool(args.updates_eml_file)
    if args.retry_failed and updates_mode_enabled:
        print("[ERROR] --retry-failed 与 --updates-eml-file 不能同时使用。")
        return 1

    bearer = (config.get("bearer_token") or "").strip()
    cookie_file = str(config.get("cookie_file") or "cookie.txt")
    cookie = ""
    need_cookie = not updates_mode_enabled
    if need_cookie:
        try:
            cookie = load_cookie_from_file(root, cookie_file)
        except Exception as exc:
            print(f"[ERROR] 无法读取 cookie 文件: {exc}")
            return 1
        if not cookie:
            print("[ERROR] cookie 文件内容为空。")
            return 1

    print(f"Asset Store Downloader - wdwxy12345@gmail.com")
    download_dir = args.download_dir or config.get("download_dir") or "downloads"
    download_root = (root / download_dir).resolve()
    download_root.mkdir(parents=True, exist_ok=True)

    timeout = int(config.get("request_timeout_sec") or 60)
    download_retries = int(config.get("download_retries") or 5)
    download_retries = max(1, download_retries)
    limit = int(config.get("limit") or 100)
    order_by = str(config.get("order_by") or "name")
    order = str(config.get("order") or "asc")
    purchase_api = str(config.get("purchase_api") or DEFAULT_CONFIG["purchase_api"])
    download_api_template = str(
        config.get("download_api_template")
        or DEFAULT_CONFIG["download_api_template"]
    )

    max_workers = int(args.max_workers or config.get("max_workers") or 3)
    max_workers = max(1, max_workers)
    purchases_export_file = str(
        args.purchases_export_file
        or config.get("purchases_export_file")
        or "purchases_snapshot.json"
    )
    purchases_export_path = (root / purchases_export_file).resolve()
    updates_eml_path: Optional[Path] = None
    updates_snapshot_path: Optional[Path] = None
    if updates_mode_enabled:
        updates_eml_path = (root / str(args.updates_eml_file)).resolve()
        updates_snapshot_file = str(args.updates_snapshot_file or purchases_export_file)
        updates_snapshot_path = (root / updates_snapshot_file).resolve()

    need_purchase_api = not args.retry_failed and not updates_mode_enabled
    if need_purchase_api and not bearer:
        print("[ERROR] bearer_token 为空，请在配置文件中填写。")
        return 1

    session: Optional[requests.Session] = None
    if need_purchase_api:
        session = build_purchase_session(bearer=bearer, cookie=cookie)

    failed_log_path = (root / args.failed_log).resolve()
    existing_failed_by_id: Dict[int, Dict] = {}
    deferred_failed_ids: set[int] = set()
    recovered_deferred_ids: set[int] = set()
    retry_remaining_by_id: Dict[int, Dict] = {}
    retry_remaining_order: List[int] = []

    def persist_retry_queue() -> None:
        if not args.retry_failed:
            return
        current_items = [
            retry_remaining_by_id[item_id]
            for item_id in retry_remaining_order
            if item_id in retry_remaining_by_id
        ]
        write_retry_targets(failed_log_path, current_items)

    if not args.retry_failed and not updates_mode_enabled and failed_log_path.exists():
        try:
            existing_failed_items = load_retry_targets(failed_log_path)
            for old_item in existing_failed_items:
                old_id_raw = old_item.get("packageId")
                try:
                    old_id = int(old_id_raw)
                except Exception:
                    continue
                existing_failed_by_id[old_id] = old_item
            deferred_failed_ids = set(existing_failed_by_id.keys())
            if deferred_failed_ids:
                print(
                    f"[INFO] 检测到历史失败项 {len(deferred_failed_ids)} 个，"
                    "本次将排到下载队列末尾。"
                )
        except Exception as exc:
            print(f"[WARN] 读取历史失败清单失败，已忽略: {exc}")

    if args.retry_failed:
        print(f"[INFO] 仅重试失败项: {failed_log_path}")
        purchases = load_retry_targets(failed_log_path)
        print(f"[INFO] 加载失败项完成，共 {len(purchases)} 条。")
        for item in purchases:
            package_id_raw = item.get("packageId")
            try:
                package_id_int = int(package_id_raw)
            except Exception:
                continue
            if package_id_int in retry_remaining_by_id:
                continue
            retry_remaining_order.append(package_id_int)
            retry_remaining_by_id[package_id_int] = {
                "packageId": package_id_int,
                "displayName": str(item.get("displayName") or f"asset_{package_id_int}"),
                "error": str(item.get("error") or "pending"),
            }
    elif updates_mode_enabled:
        assert updates_eml_path is not None
        assert updates_snapshot_path is not None

        try:
            updated_asset_names = extract_updated_asset_names_from_eml(updates_eml_path)
        except Exception as exc:
            print(f"[ERROR] 解析更新通知邮件失败: {exc}")
            return 1

        if not updated_asset_names:
            print(f"[WARN] 未从邮件中解析到更新素材: {updates_eml_path}")
            return 0

        try:
            snapshot_payload = json.loads(
                updates_snapshot_path.read_text(encoding="utf-8")
            )
            if not isinstance(snapshot_payload, list):
                raise ValueError("快照文件不是列表格式")
        except Exception as exc:
            print(
                f"[ERROR] 无法读取更新模式快照: {updates_snapshot_path}, 错误: {exc}"
            )
            return 1

        purchases, unmatched_updates = filter_purchases_for_updates(
            purchases=snapshot_payload, updated_asset_names=updated_asset_names
        )
        print(
            f"[INFO] 邮件中解析到更新素材 {len(updated_asset_names)} 个，"
            f"快照匹配成功 {len(purchases)} 个。"
        )
        if unmatched_updates:
            preview = ", ".join(unmatched_updates[:10])
            print(
                "[WARN] 以下素材未在快照中匹配到（仅显示前10个）: "
                f"{preview}"
            )
        if not purchases:
            print("[ERROR] 未匹配到任何可下载素材。")
            return 1
        update_items: List[Dict] = []
        update_ids: Set[int] = set()
        skipped_cn = 0
        for item in purchases:
            package_id_raw = item.get("packageId")
            try:
                package_id_int = int(package_id_raw)
            except Exception:
                continue
            if package_id_int >= 20000000:
                skipped_cn += 1
                continue
            if package_id_int in update_ids:
                continue
            update_ids.add(package_id_int)
            update_items.append(
                {
                    "packageId": package_id_int,
                    "displayName": str(item.get("displayName") or f"asset_{package_id_int}"),
                    "error": "pending_update_from_eml",
                }
            )

        existing_items: List[Dict] = []
        if failed_log_path.exists():
            try:
                existing_items = load_retry_targets(failed_log_path)
            except Exception as exc:
                print(f"[WARN] 读取原失败清单失败，将覆盖写入: {exc}")

        merged_items = list(update_items)
        for old_item in existing_items:
            old_id_raw = old_item.get("packageId")
            try:
                old_id = int(old_id_raw)
            except Exception:
                continue
            if old_id in update_ids:
                continue
            merged_items.append(old_item)

        write_retry_targets(failed_log_path, merged_items)
        print(
            f"[DONE] 已把 EML 更新项写入失败清单队列前部: 新增 {len(update_items)} 个, "
            f"保留历史 {max(0, len(merged_items) - len(update_items))} 个, "
            f"跳过CN特供 {skipped_cn} 个。"
        )
        print(f"[INFO] 队列文件: {failed_log_path}")
        return 0
    else:
        print("[INFO] 开始拉取已购买资产列表...")
        assert session is not None
        try:
            purchases = fetch_all_purchases(
                session=session,
                purchase_api=purchase_api,
                limit=limit,
                order_by=order_by,
                order=order,
                timeout=timeout,
            )
            print(f"[INFO] 列表拉取完成，共 {len(purchases)} 条。")
        except Exception as exc:
            print(f"[WARN] 拉取已购列表失败: {exc}")
            if purchases_export_path.exists():
                try:
                    purchases = json.loads(
                        purchases_export_path.read_text(encoding="utf-8")
                    )
                    if not isinstance(purchases, list):
                        raise ValueError("快照不是列表格式")
                    print(
                        "[WARN] 已回退使用本地快照列表继续下载: "
                        f"{purchases_export_path} (共 {len(purchases)} 条)"
                    )
                except Exception as load_exc:
                    print(
                        "[ERROR] 已购列表拉取失败，且本地快照不可用: "
                        f"{purchases_export_path}, 错误: {load_exc}"
                    )
                    return 1
            else:
                print(
                    "[ERROR] 已购列表拉取失败，且本地快照不存在: "
                    f"{purchases_export_path}"
                )
                return 1

    if not args.retry_failed and not updates_mode_enabled:
        purchases_export_path.write_text(
            json.dumps(purchases, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[INFO] 素材列表已导出: {purchases_export_path}")

    force_redownload_existing = bool(args.force_redownload_existing)
    if args.retry_failed and force_redownload_existing:
        print("[INFO] 失败重试已启用强制重下：已存在文件不会跳过。")

    downloaded = 0
    skipped = 0
    failed = 0
    failed_items: List[Dict] = []
    to_download: List[Dict] = []
    retry_queue_changed_before_download = False

    for idx, item in enumerate(purchases, start=1):
        package_id = item.get("packageId")
        display_name = str(item.get("displayName") or f"asset_{package_id}")

        if not package_id:
            print(f"[WARN] 第 {idx} 条缺少 packageId，已跳过。")
            skipped += 1
            continue
        try:
            package_id_int = int(package_id)
        except Exception:
            print(f"[WARN] 第 {idx} 条 packageId 非法({package_id})，已跳过。")
            skipped += 1
            continue

        if package_id_int >= 20000000:
            print(f"[SKIP] 跳过【CN特供资源】 {package_id_int} {display_name}")
            skipped += 1
            continue

        filename = sanitize_filename(display_name) + ".unitypackage"
        output_file = download_root / filename

        already_exists = output_file.exists()
        if already_exists and not force_redownload_existing:
            print(f"[SKIP] 已存在，跳过: {filename}")
            skipped += 1
            if package_id_int in deferred_failed_ids:
                recovered_deferred_ids.add(package_id_int)
            if args.retry_failed and package_id_int in retry_remaining_by_id:
                retry_remaining_by_id.pop(package_id_int, None)
                retry_queue_changed_before_download = True
            continue

        to_download.append(
            {
                "index": idx,
                "total": len(purchases),
                "packageId": package_id_int,
                "displayName": display_name,
                "filename": filename,
                "outputFile": output_file,
                "isDeferredFailed": package_id_int in deferred_failed_ids,
                "isForceRedownload": already_exists and force_redownload_existing,
            }
        )

    if retry_queue_changed_before_download:
        persist_retry_queue()

    if deferred_failed_ids:
        normal_tasks = [t for t in to_download if not t["isDeferredFailed"]]
        deferred_tasks = [t for t in to_download if t["isDeferredFailed"]]
        to_download = normal_tasks + deferred_tasks
        print(
            f"[INFO] 已将历史失败项延后: 普通任务 {len(normal_tasks)} 个，"
            f"历史失败任务 {len(deferred_tasks)} 个。"
        )

    print(
        f"[INFO] 待下载 {len(to_download)} 个，"
        f"并发={max_workers}，已跳过={skipped}"
    )

    def worker(task: Dict) -> Dict:
        worker_session = build_download_session(cookie=cookie)
        try:
            if task["outputFile"].exists() and not force_redownload_existing:
                return {"status": "skipped", "task": task}
            download_one_asset(
                session=worker_session,
                download_api_template=download_api_template,
                package_id=task["packageId"],
                output_file=task["outputFile"],
                timeout=timeout,
                retries=download_retries,
            )
            return {"status": "ok", "task": task}
        except Exception as exc:
            return {"status": "failed", "task": task, "error": str(exc)}
        finally:
            worker_session.close()

    interrupted = False
    executor = ThreadPoolExecutor(max_workers=max_workers)
    futures = [executor.submit(worker, task) for task in to_download]
    try:
        for future in as_completed(futures):
            result = future.result()
            task = result["task"]
            status = result.get("status")
            if status == "ok":
                downloaded += 1
                if task.get("isDeferredFailed"):
                    recovered_deferred_ids.add(task["packageId"])
                if args.retry_failed:
                    retry_remaining_by_id.pop(task["packageId"], None)
                    persist_retry_queue()
                status_tag = "RE-DL" if task.get("isForceRedownload") else "OK"
                print(
                    f"[{status_tag}] ({task['index']}/{task['total']}) "
                    f"[{task['packageId']}] {task['displayName']} -> {task['filename']}"
                )
            elif status == "skipped":
                skipped += 1
                if task.get("isDeferredFailed"):
                    recovered_deferred_ids.add(task["packageId"])
                if args.retry_failed:
                    retry_remaining_by_id.pop(task["packageId"], None)
                    persist_retry_queue()
                print(
                    f"[SKIP] ({task['index']}/{task['total']}) "
                    f"已存在，跳过: [{task['packageId']}] {task['filename']}"
                )
            else:
                failed += 1
                err = str(result.get("error") or "unknown error")
                failed_items.append(
                    {
                        "packageId": task["packageId"],
                        "displayName": task["displayName"],
                        "error": err,
                    }
                )
                if args.retry_failed:
                    retry_remaining_by_id[task["packageId"]] = {
                        "packageId": task["packageId"],
                        "displayName": task["displayName"],
                        "error": err,
                    }
                    if task["packageId"] not in retry_remaining_order:
                        retry_remaining_order.append(task["packageId"])
                    persist_retry_queue()
                print(
                    f"[FAIL] ({task['index']}/{task['total']}) "
                    f"{task['displayName']} (packageId={task['packageId']}): {err}"
                )
    except KeyboardInterrupt:
        interrupted = True
        print("\n[STOP] 检测到 Ctrl+C，正在停止下载任务...")
        executor.shutdown(wait=False, cancel_futures=True)
    finally:
        if not interrupted:
            executor.shutdown(wait=True)

    if args.retry_failed:
        merged_failed_items = [
            retry_remaining_by_id[item_id]
            for item_id in retry_remaining_order
            if item_id in retry_remaining_by_id
        ]
        write_retry_targets(failed_log_path, merged_failed_items)
    else:
        merged_failed_by_id: Dict[int, Dict] = {}
        for old_id, old_item in existing_failed_by_id.items():
            if old_id not in recovered_deferred_ids:
                merged_failed_by_id[old_id] = old_item
        for item in failed_items:
            try:
                new_id = int(item.get("packageId"))
            except Exception:
                continue
            merged_failed_by_id[new_id] = item
        merged_failed_items = list(merged_failed_by_id.values())
        write_retry_targets(failed_log_path, merged_failed_items)

    if merged_failed_items:
        print(f"[INFO] 失败清单已写入: {failed_log_path}")
    else:
        print(f"[INFO] 本次无失败项，已清空失败清单: {failed_log_path}")

    print(
        "[DONE] 下载结束: "
        f"成功={downloaded}, 跳过={skipped}, 失败={failed}, "
        f"下载目录={download_root}"
    )
    if interrupted:
        print("[DONE] 已由用户中断。")
        return 130
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
