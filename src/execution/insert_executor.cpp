//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = nullptr;
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  // Query table metadata by OID
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // If there is a child executor (at most one child plan is allowed), init the child
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Query table to be inserted into
  TableHeap *table_ = table_info_->table_.get();
  BUSTUB_ASSERT(table_info_ != nullptr, "The table info is nullptr.");
  const Schema &schema = table_info_->schema_;
  // Query the plan to check the type of insert
  if (plan_->IsRawInsert()) {
    // Raw insert
    // It is possible that there are multiple records to be inserted
    // Insert into the table directly and update the indexes
    for (auto &vals : plan_->RawValues()) {
      table_->InsertTuple(Tuple(vals, &schema), rid, exec_ctx_->GetTransaction());
    }
  } else {
    // Insert from a sub-query
    // First execute the child plan
    // Get the results from the child executor
    // Insert the results into the current plan and update the indexes
  }

  return false;
}

}  // namespace bustub
