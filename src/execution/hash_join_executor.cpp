//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
  join_results_.reserve(20);
}

void HashJoinExecutor::Init() {
  // Init the left and right child executors
  BUSTUB_ASSERT(left_child_ != nullptr, "Left child executor is null.");
  BUSTUB_ASSERT(right_child_ != nullptr, "Right child executor is null.");
  left_child_->Init();
  right_child_->Init();
  // Compute join
  Tuple left_tuple;
  RID left_rid;
  const Schema *left_schema = left_child_->GetOutputSchema();
  // Phase #1: Build the hash table
  while (left_child_->Next(&left_tuple, &left_rid)) {
    // Get the key by evaluating the left_key_expression
    HashJoinKey left_hash_key;
    left_hash_key.column_value_ =
        static_cast<const ColumnValueExpression *>(plan_->LeftJoinKeyExpression())->Evaluate(&left_tuple, left_schema);
    // Construct the value for a hash table entry
    HashJoinValue left_hash_value;
    left_hash_value.tuples_.emplace_back(left_tuple);
    hash_table_.Insert(left_hash_key, left_hash_value);
  }
  // Phase #2: Probe and store the results
  Tuple right_tuple;
  RID right_rid;
  HashJoinKey right_hash_key;
  const Schema *output_schema = GetOutputSchema();
  const Schema *right_schema = right_child_->GetOutputSchema();
  std::vector<Value> output_values;
  while (right_child_->Next(&right_tuple, &right_rid)) {
    // Get the key by evaluating the right_key_expression
    right_hash_key.column_value_ = static_cast<const ColumnValueExpression *>(plan_->RightJoinKeyExpression())
                                       ->Evaluate(&right_tuple, right_schema);
    // Check whether the key for this tuple exists in the hash table
    if (hash_table_.HasKey(right_hash_key)) {
      // If the key exists
      // Compare with all the tuples in this bucket
      for (const auto &left_tuple_temp : hash_table_.GetValue(right_hash_key).tuples_) {
        // Combine and produce a tuple as a join result

        output_values.clear();
        for (auto &col : output_schema->GetColumns()) {
          output_values.emplace_back(
              col.GetExpr()->EvaluateJoin(&left_tuple_temp, left_schema, &right_tuple, right_schema));
        }
        // Once a tuple is found, store it
        join_results_.emplace_back(Tuple(output_values, output_schema));
      }
    }
  }
  // Initialize the result iterator
  join_results_iter_ = join_results_.begin();
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (join_results_iter_ == join_results_.end()) {
    // No more tuples
    return false;
  }
  *tuple = *join_results_iter_;
  *rid = tuple->GetRid();
  ++join_results_iter_;
  return true;
}

}  // namespace bustub
