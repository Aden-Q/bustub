//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  num_tuples_remain_ = plan->GetLimit();
}

void LimitExecutor::Init() {
  BUSTUB_ASSERT(child_executor_ != nullptr, "The child executor is a nullptr.");
  child_executor_->Init();
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (num_tuples_remain_ == 0) {
    return false;
  }
  if (child_executor_->Next(tuple, rid)) {
    num_tuples_remain_--;
    return true;
  }
  // If a tuple is not produced, it can be
  // the case that a child node is unable to produce
  // a new tuple at the moment, do not decrement the counter
  return false;
}

}  // namespace bustub
