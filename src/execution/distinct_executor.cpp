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
    hash_value.tuples_.emplace_back(temp_tuple);
    if (!hash_table_.HasKey(hash_key)) {
      // If the current hash table does not have the key
      // Insert it into the hash table and emit the result
      hash_table_.Insert(hash_key, hash_value);
      *tuple = temp_tuple;
      *rid = temp_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
