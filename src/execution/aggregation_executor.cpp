//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx), aht_(plan->GetAggregates(), plan->GetAggregateTypes()), aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
}

void AggregationExecutor::Init() {
  BUSTUB_ASSERT(child_ != nullptr, "The child executor is a nullptr.");
  child_->Init();
  Tuple tuple_temp;
  RID rid_temp;
  // Phase #1: Build the aggregation hash table
  // Gather the results produced by a child executor
  while (child_->Next(&tuple_temp, &rid_temp)) {
    // This statement performs group by automatically
    aht_.InsertCombine(MakeAggregateKey(&tuple_temp), MakeAggregateValue(&tuple_temp));
  }
  // Initialize the aggregation hash table iterator
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    // There is no more tuples
    return false;
  }
  std::vector<Value> output_values;
  const Schema *output_schema = GetOutputSchema();
  // A single having clause
  // Iterate through the hash table to find tuples satisfying the having clause
  while (aht_iterator_ != aht_.End()) {
    if (plan_->GetHaving() == nullptr ||
        plan_->GetHaving()
            ->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
            .GetAs<bool>()) {
      // The current tuple satisfy the having clause
      // Emit it
      output_values.clear();
      for (auto &col : output_schema->GetColumns()) {
        output_values.emplace_back(
            col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
      }
      *tuple = Tuple(output_values, output_schema);
      ++aht_iterator_;
      return true;
    }
    // Check the next tuple
    ++aht_iterator_;
  }
  // In the case that we cannot find a tuple satisfying the having clause
  // Or aht_iterator_ has reached the end
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
