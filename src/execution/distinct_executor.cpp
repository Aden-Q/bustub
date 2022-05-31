//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DistinctExecutor::Init() {
  // Init the left and right child executors
  BUSTUB_ASSERT(child_executor_ != nullptr, "The child executor is a nullptr.");
  child_executor_->Init();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple temp_tuple;
  RID temp_rid;
  // Execute the child executor
  while (child_executor_->Next(&temp_tuple, &temp_rid)) {
    // Fetch the next tuple from the child executor
    HashDistinctKey hash_key = MakeHashDistinctKey(&temp_tuple);
    HashDistinctValue hash_value;
    hash_value.tuples_.push_back(temp_tuple);
    if (!hash_table_.HasKey(hash_key)) {
      // If the current hash table does not have the key
      // Insert it into the hash table and emit the result
      hash_table_.Insert(hash_key, hash_value);
      *tuple = temp_tuple;
      *rid = temp_rid;
      return true;
    }
    // else {
    //   // Check whether the tuple resides in the bucket
    //   // Iterate through the bucket to check all the existing tuples
    //   for (const auto &next_tuple : hash_table_.GetValue(hash_key).tuples_) {
    //     for (size_t col_idx = 0; col_idx < output_schema->GetColumnCount(); col_idx++) {
    //       if (output_schema->GetColumn(col_idx)
    //               .GetExpr()
    //               ->Evaluate(&temp_tuple, child_executor_->GetOutputSchema())
    //               .CompareEquals(next_tuple.GetValue(output_schema, col_idx)) != CmpBool::CmpTrue) {
    //         hash_table_.Insert(hash_key, hash_value);
    //         *tuple = temp_tuple;
    //         *rid = temp_rid;
    //         return true;
    //       }
    //     }
    //   }
    // }
  }
  return false;
}

}  // namespace bustub
