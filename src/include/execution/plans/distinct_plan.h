//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_plan.h
//
// Identification: src/include/execution/plans/distinct_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/util/hash_util.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Distinct removes duplicate rows from the output of a child node.
 */
class DistinctPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new DistinctPlanNode instance.
   * @param child The child plan from which tuples are obtained
   */
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, {child}) {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::Distinct; }

  /** @return The child plan node */
  const AbstractPlanNode *GetChildPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Distinct should have at most one child plan.");
    return GetChildAt(0);
  }
};

/** HashDistinctKey represents a key in a hash table */
struct HashDistinctKey {
  /** A single attribute to be joined on  */
  std::vector<Value> column_values_;

  /**
   * Compare two hash distinct keys for equality
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join key have
   */
  bool operator==(const HashDistinctKey &other) const {
    for (uint32_t i = 0; i < other.column_values_.size(); i++) {
      if (column_values_[i].CompareEquals(other.column_values_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

/** HashDistinctValue represents a value of an hash table entry */
struct HashDistinctValue {
  /** The values are full tuples with the same hash key */
  // It might be the case that attributes to be hashed are different
  // but the hash key is the same (hashed to the same partition)
  // So we gather all those tuples in the same partition as a collection
  std::vector<Tuple> tuples_;
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashDistinctKey */
template <>
struct hash<bustub::HashDistinctKey> {
  std::size_t operator()(const bustub::HashDistinctKey &hash_key) const {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.column_values_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std
