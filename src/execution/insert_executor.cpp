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
  // If there is a child executor (at most one child plan is allowed), init the child
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Query table to be inserted into
  BUSTUB_ASSERT(table_info_ != nullptr, "Table info is a nullptr.");
  TableHeap *table = table_info_->table_.get();
  const Schema &schema = table_info_->schema_;
  // LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  // Transaction *txn = GetExecutorContext()->GetTransaction();
  // Query the plan to check the type of insert
  if (plan_->IsRawInsert()) {
    // Raw insert
    // It is possible that there are multiple records to be inserted
    // Read the tuples to be inserted
    for (auto &vals : plan_->RawValues()) {
      Tuple tuple_temp = Tuple(vals, &schema);
      RID rid_temp = tuple_temp.GetRid();
      if (table->InsertTuple(tuple_temp, &rid_temp, exec_ctx_->GetTransaction())) {
        // txn->AppendTableWriteRecord(TableWriteRecord(rid_temp, WType::INSERT, tuple_temp, table));
        // Update indexes for each inserted row
        for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          index_info->index_->InsertEntry(
              tuple_temp.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
              rid_temp, exec_ctx_->GetTransaction());
          // txn->AppendIndexWriteRecord(IndexWriteRecord(rid_temp, table_info_->oid_, WType::INSERT, tuple_temp,
          //                                              tuple_temp, index_info->index_oid_, exec_ctx_->GetCatalog()));
        }
      } else {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor: not enough space.");
      }
    }
  } else {
    // Insert from a sub-query
    // First execute the child plan
    // Get the results from the child executor
    Tuple tuple_temp;
    RID rid_temp;
    while (child_executor_->Next(&tuple_temp, &rid_temp)) {
      if (table->InsertTuple(tuple_temp, &rid_temp, exec_ctx_->GetTransaction())) {
        // txn->AppendTableWriteRecord(TableWriteRecord(rid_temp, WType::INSERT, tuple_temp, table));
        // Update indexes for each inserted row
        for (auto index_info : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
          index_info->index_->InsertEntry(
              tuple_temp.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
              rid_temp, exec_ctx_->GetTransaction());
          // txn->AppendIndexWriteRecord(IndexWriteRecord(rid_temp, table_info_->oid_, WType::INSERT, tuple_temp,
          //                                              tuple_temp, index_info->index_oid_, exec_ctx_->GetCatalog()));
        }
      } else {
        throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor: not enough space.");
      }
    }
  }
  return false;
}

}  // namespace bustub
