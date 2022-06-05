//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = nullptr;
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  // Query table and indexes metadata by OID
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_info_vec_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  // Init the child
  BUSTUB_ASSERT(child_executor_ != nullptr, "Child executor is null.");
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Query table to be inserted into
  BUSTUB_ASSERT(table_info_ != nullptr, "Table info is a nullptr.");
  TableHeap *table = table_info_->table_.get();
  // Query table schema
  const Schema &schema = table_info_->schema_;
  Tuple tuple_temp;
  RID rid_temp;
  std::vector<std::pair<Tuple, RID>> tuples;
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
  Transaction *txn = GetExecutorContext()->GetTransaction();
  // Get tuples to be updated from a child executor
  while (child_executor_->Next(&tuple_temp, &rid_temp)) {
    // Add the current tuple to the write set of the transaction
    tuples.emplace_back(tuple_temp, rid_temp);
  }
  // Update the table
  for (auto &next_tuple : tuples) {
    // Lock on each tuple to be updated
    if (txn->IsSharedLocked(next_tuple.second)) {
      lock_mgr->LockUpgrade(txn, next_tuple.second);
    } else {
      lock_mgr->LockExclusive(txn, next_tuple.second);
    }
    Tuple updated_tuple = GenerateUpdatedTuple(next_tuple.first);
    // The updated tuple and the old tuple has the same RID
    table->UpdateTuple(updated_tuple, next_tuple.second, exec_ctx_->GetTransaction());
    // txn->AppendTableWriteRecord(TableWriteRecord(next_tuple.second, WType::UPDATE, updated_tuple, table));
    // Update indexes on each insertion
    // By deleting the old index entry and insert the updated one
    for (auto index_info : index_info_vec_) {
      index_info->index_->DeleteEntry(
          next_tuple.first.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
          next_tuple.second, exec_ctx_->GetTransaction());
      txn->AppendIndexWriteRecord(IndexWriteRecord(rid_temp, table_info_->oid_, WType::DELETE, updated_tuple,
                                                   next_tuple.first, index_info->index_oid_, exec_ctx_->GetCatalog()));
      index_info->index_->InsertEntry(
          updated_tuple.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
          next_tuple.second, exec_ctx_->GetTransaction());
      txn->AppendIndexWriteRecord(IndexWriteRecord(rid_temp, table_info_->oid_, WType::INSERT, updated_tuple,
                                                   next_tuple.first, index_info->index_oid_, exec_ctx_->GetCatalog()));
    }
  }
  // The query plan does not produce any tuple, so always returns false
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
