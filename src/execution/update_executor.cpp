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
  // If there is a child executor (at most one child plan is allowed), init the child
  if (child_executor_ != nullptr) {
    child_executor_->Init();
  }
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // Query table to be inserted into
  BUSTUB_ASSERT(table_info_ != nullptr, "Table info is a nullptr.");
  TableHeap *table_ = table_info_->table_.get();
  // Query table schema
  const Schema &schema = table_info_->schema_;
  Tuple tuple_temp;
  RID rid_temp;
  std::vector<std::pair<Tuple, RID>> tuples;
  // Get the new tuples from a child executor
  while (child_executor_->Next(&tuple_temp, &rid_temp)) {
    tuples.emplace_back(tuple_temp, rid_temp);
  }
  // Update the table
  for (auto &next_tuple : tuples) {
    Tuple updated_tuple = GenerateUpdatedTuple(next_tuple.first);
    // The updated tuple and the old tuple has the same RID
    table_->UpdateTuple(updated_tuple, next_tuple.second, exec_ctx_->GetTransaction());
    // Update indexes on each insertion
    // By deleting the old index entry and insert the updated one
    for (auto index_info : index_info_vec_) {
      index_info->index_->DeleteEntry(
          next_tuple.first.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
          next_tuple.second, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(
          updated_tuple.KeyFromTuple(schema, *index_info->index_->GetKeySchema(), index_info->index_->GetKeyAttrs()),
          next_tuple.second, exec_ctx_->GetTransaction());
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
