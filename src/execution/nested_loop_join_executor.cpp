//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
  // Give the result array some initial capacity
  results_.reserve(50);
}

void NestedLoopJoinExecutor::Init() {
  // Init the left and right child executors
  BUSTUB_ASSERT(left_executor_ != nullptr, "Left child executor is null.");
  BUSTUB_ASSERT(right_executor_ != nullptr, "Right child executor is null.");
  // Compute join
  Tuple left_tuple;
  Tuple right_tuple;
  std::vector<Value> output_values;
  RID left_rid;
  RID right_rid;
  int64_t cnt = 0;
  left_executor_->Init();
  right_executor_->Init();
  const Schema *left_schema = left_executor_->GetOutputSchema();
  const Schema *right_schema = right_executor_->GetOutputSchema();
  const Schema *output_schema = GetOutputSchema();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    // Check all the tuples produced by the right executor
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // Evaluate the predicate to check the join condition
      // If the predicate is nullptr, meaning that it is a full join, produce all combinations
      // Otherwise if the predicate evalutes to be true, emit the tuple
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        // Produce the output tuple
        output_values.clear();
        for (auto &col : output_schema->GetColumns()) {
          output_values.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
        }
        results_.emplace_back(Tuple(output_values, output_schema), RID(cnt++));
      }
    }
  }
  // Initialize the iterator
  results_iter_ = results_.begin();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (results_iter_ == results_.end()) {
    // There is no more tuples
    return false;
  }
  *tuple = results_iter_->first;
  *rid = results_iter_->second;
  // Increment the iterator, pointing to the next tuple
  results_iter_++;
  return true;
}

}  // namespace bustub
