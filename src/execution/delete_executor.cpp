//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = nullptr;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  // Query table and indexes metadata by OID
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // Init the child
  BUSTUB_ASSERT(child_executor_ != nullptr, "Child executor is null.");
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Query table to be inserted into
  BUSTUB_ASSERT(table_info_ != nullptr, "Table info is a nullptr.");
  TableHeap *table_ = table_info_->table_.get();
  // Query table schema
  const Schema &schema = table_info_->schema_;
  Tuple tuple_temp;
  RID rid_temp;
  std::vector<std::pair<Tuple, RID>> tuples;
  // Get tuples to be updated from a child executor
  while (child_executor_->Next(&tuple_temp, &rid_temp)) {
    tuples.emplace_back(tuple_temp, rid_temp);
  }
  // Delete tuples from the table
  for (auto &next_tuple : tuples) {
    table_->MarkDelete(next_tuple.second, exec_ctx_->GetTransaction());
    // Delete from indexes
    for (auto index_info : index_info_vec_) {
      index_info->index_->DeleteEntry(
          next_tuple.first.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
          next_tuple.second, exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
