//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  table_info_ = nullptr;
}

void SeqScanExecutor::Init() {
  // The sequential scan plan node carries a table Id to query from
  // We then use that table get to retrieve the table from the system catalog
  // Then retrieve the table
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  // Get an iterator for the table
  table_itr_ = std::make_unique<TableIterator>(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // If no more tuples, return false
  BUSTUB_ASSERT(table_info_ != nullptr && table_itr_ != nullptr, "Either the table info or iterator is nullptr.");
  TableHeap *table_ = table_info_->table_.get();
  if (*table_itr_ == table_->End()) {
    return false;
  }
  // Produce the next tuple
  std::vector<Value> vals;
  const Schema *output_schema = plan_->OutputSchema();
  Schema &schema = table_info_->schema_;
  vals.reserve(output_schema->GetColumnCount());
  // For each column in the output schema, used the table iterator
  // given by the query plan, to evaluate a column value for that tuple
  // And populate each column
  for (size_t col_idx = 0; col_idx < output_schema->GetColumnCount(); col_idx++) {
    vals.push_back(output_schema->GetColumn(col_idx).GetExpr()->Evaluate(&(**table_itr_), &schema));
  }
  // Populate the tuple (fill the content with the current row)
  *tuple = Tuple(vals, output_schema);
  *rid = (*table_itr_)->GetRid();
  (*table_itr_)++;
  // Evaluate the (optional) predicate given by the query plan node
  const AbstractExpression *predicate = plan_->GetPredicate();
  if (predicate != nullptr && !predicate->Evaluate(tuple, output_schema).GetAs<bool>()) {
    // If the (comparision) evaluates to False, meaning that the current row
    // does not satisfy the predicate. Skip this row and fetch the next
    return Next(tuple, rid);
  }
  // Otherwise, either it is because the predicate is nullptr,
  // or the current row satisfies the predicate (evaluate to true)
  return true;
}

}  // namespace bustub
