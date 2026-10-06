"""子块映射到父块后执行多路 RRF，平分时按父块 ID 保持结果稳定。"""


# 作用：保留各路父块的首次召回名次，并按 RRF 分数融合 Dense 与所有 Sparse 路。
def map_to_parent_and_rrf(dense_child_results: list, sparse_child_results: list, k: int = 60, extra_sparse_results: list[list] | None = None) -> list[tuple[str, float]]:
    # 作用：将一路子块排名转成父块的最佳排名，避免同一父块重复加分。
    def get_parent_ranks(children: list) -> dict[str, int]:
        ranks = {}
        for rank, child in enumerate(children, start=1):
            ranks.setdefault(child["parent_id"], rank)
        return ranks

    ranks_by_route = [get_parent_ranks(route) for route in [dense_child_results, sparse_child_results, *(extra_sparse_results or [])]]
    scores = {}
    for ranks in ranks_by_route:
        for parent_id, rank in ranks.items():
            scores[parent_id] = scores.get(parent_id, 0.0) + 1.0 / (k + rank)

    # 作用：按融合分数降序、父块 ID 升序提供可重复的排序键。
    def sort_key(item: tuple[str, float]) -> tuple[float, str]:
        return -item[1], item[0]

    return sorted(scores.items(), key=sort_key)
