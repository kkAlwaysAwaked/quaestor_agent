# return formatted_results = [
#         {
#             "id": point.id,  # 子文档的 ID（用于 RRF 打分）
#             "parent_id": point.payload["parent_id"],
#         }
#         for point in raw_results
#     ]

def map_to_parent_and_rrf(
    dense_child_results: list,
    sparse_child_results: list,
    k: int = 60,
    extra_sparse_results: list[list] | None = None,
):
    """
    先将子文档映射为父文档（保留最高排名），再进行 RRF 融合打分。
    支持多路 Sparse 结果叠加（如 Agent query + LLM 关键词）。
    返回排序后的 [(parent_id, rrf_score), ...] 列表。
    """
    def get_parent_ranks(children_list):
        parent_ranks = {}
        for rank, child in enumerate(children_list):
            pid = child["parent_id"]
            if pid not in parent_ranks:
                parent_ranks[pid] = rank + 1
        return parent_ranks

    dense_parent_ranks = get_parent_ranks(dense_child_results)
    sparse_lists = [sparse_child_results]
    if extra_sparse_results:
        sparse_lists.extend(extra_sparse_results)

    all_parents = set(dense_parent_ranks.keys())
    for sparse_list in sparse_lists:
        all_parents |= set(get_parent_ranks(sparse_list).keys())

    rrf_scores = {}
    for pid in all_parents:
        score = 0.0
        if pid in dense_parent_ranks:
            score += 1.0 / (k + dense_parent_ranks[pid])
        for sparse_list in sparse_lists:
            sparse_ranks = get_parent_ranks(sparse_list)
            if pid in sparse_ranks:
                score += 1.0 / (k + sparse_ranks[pid])
        rrf_scores[pid] = score

    return sorted(rrf_scores.items(), key=lambda item: item[1], reverse=True)





