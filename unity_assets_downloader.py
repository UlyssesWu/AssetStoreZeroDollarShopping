# by Ulysses, wdwxy12345@gmail.com
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests


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

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, stream=True, timeout=timeout)
            resp.raise_for_status()
            resp = resolve_download_response(session, resp, timeout)
            resp.raise_for_status()

            tmp_file = output_file.with_suffix(output_file.suffix + ".part")
            with tmp_file.open("wb") as f:
                for chunk in resp.iter_content(chunk_size = 2 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
            tmp_file.replace(output_file)
            return
        except Exception as exc:
            last_err = exc
            if attempt < retries:
                wait_sec = attempt * 2
                print(
                    f"[WARN] 下载失败 packageId={package_id},"
                    f" 第 {attempt} 次重试后等待 {wait_sec}s: {exc}"
                )
                time.sleep(wait_sec)
            else:
                break

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
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Referer": "https://assetstore.unity.com/",
            "Origin": "https://assetstore.unity.com",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0"
            ),
            "Cookie": cookie,
        }
    )
    return session


def load_retry_targets(failed_log_path: Path) -> List[Dict]:
    if not failed_log_path.exists():
        raise FileNotFoundError(f"失败清单不存在: {failed_log_path}")
    payload = json.loads(failed_log_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"失败清单格式错误，期望 list: {failed_log_path}")
    return payload


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
    args = parser.parse_args()

    root = Path.cwd()
    config_path = (root / args.config).resolve()
    config = load_config(config_path)

    bearer = (config.get("bearer_token") or "").strip()
    cookie_file = str(config.get("cookie_file") or "cookie.txt")
    try:
        cookie = load_cookie_from_file(root, cookie_file)
    except Exception as exc:
        print(f"[ERROR] 无法读取 cookie 文件: {exc}")
        return 1

    if not bearer:
        print("[ERROR] bearer_token 为空，请在配置文件中填写。")
        return 1
    if not cookie:
        print("[ERROR] cookie 文件内容为空。")
        return 1

    print(f"Asset Store Downloader - wdwxy12345@gmail.com")
    download_dir = args.download_dir or config.get("download_dir") or "downloads"
    download_root = (root / download_dir).resolve()
    download_root.mkdir(parents=True, exist_ok=True)

    timeout = int(config.get("request_timeout_sec") or 60)
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

    session = build_purchase_session(bearer=bearer, cookie=cookie)

    failed_log_path = (root / args.failed_log).resolve()
    existing_failed_by_id: Dict[int, Dict] = {}
    deferred_failed_ids: set[int] = set()
    recovered_deferred_ids: set[int] = set()

    if not args.retry_failed and failed_log_path.exists():
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
    else:
        print("[INFO] 开始拉取已购买资产列表...")
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

    purchases_export_path.write_text(
        json.dumps(purchases, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"[INFO] 素材列表已导出: {purchases_export_path}")

    downloaded = 0
    skipped = 0
    failed = 0
    failed_items: List[Dict] = []
    to_download: List[Dict] = []

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

        if output_file.exists():
            print(f"[SKIP] 已存在，跳过: {filename}")
            skipped += 1
            if package_id_int in deferred_failed_ids:
                recovered_deferred_ids.add(package_id_int)
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
            }
        )

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
            if task["outputFile"].exists():
                return {"status": "skipped", "task": task}
            download_one_asset(
                session=worker_session,
                download_api_template=download_api_template,
                package_id=task["packageId"],
                output_file=task["outputFile"],
                timeout=timeout,
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
                print(
                    f"[OK] ({task['index']}/{task['total']}) "
                    f"[{task['packageId']}] {task['displayName']} -> {task['filename']}"
                )
            elif status == "skipped":
                skipped += 1
                if task.get("isDeferredFailed"):
                    recovered_deferred_ids.add(task["packageId"])
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
    failed_log_path.write_text(
        json.dumps(merged_failed_items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
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
