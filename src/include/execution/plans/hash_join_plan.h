//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_plan.h
//
// Identification: src/include/execution/plans/hash_join_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * Hash join performs a JOIN operation with a hash table.
 */
class HashJoinPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new HashJoinPlanNode instance.
   * @param output_schema The output schema for the JOIN
   * @param children The child plans from which tuples are obtained
   * @param left_key_expression The expression for the left JOIN key
   * @param right_key_expression The expression for the right JOIN key
   */
  HashJoinPlanNode(const Schema *output_schema, std::vector<const AbstractPlanNode *> &&children,
                   const AbstractExpression *left_key_expression, const AbstractExpression *right_key_expression)
      : AbstractPlanNode(output_schema, std::move(children)),
        left_key_expression_{left_key_expression},
        right_key_expression_{right_key_expression} {}

  /** @return The type of the plan node */
  PlanType GetType() const override { return PlanType::HashJoin; }

  /** @return The expression to compute the left join key */
  const AbstractExpression *LeftJoinKeyExpression() const { return left_key_expression_; }

  /** @return The expression to compute the right join key */
  const AbstractExpression *RightJoinKeyExpression() const { return right_key_expression_; }

  /** @return The left plan node of the hash join */
  const AbstractPlanNode *GetLeftPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(0);
  }

  /** @return The right plan node of the hash join */
  const AbstractPlanNode *GetRightPlan() const {
    BUSTUB_ASSERT(GetChildren().size() == 2, "Hash joins should have exactly two children plans.");
    return GetChildAt(1);
  }

 private:
  /** The expression to compute the left JOIN key */
  const AbstractExpression *left_key_expression_;
  /** The expression to compute the right JOIN key */
  const AbstractExpression *right_key_expression_;
};

/** HashJoinKey represents a key in a hash join operation */
struct HashJoinKey {
  /** A single attribute to be joined on  */
  Value column_value_;

  /**
   * Compare two hash join keys for equality
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join key have
   */
  bool operator==(const HashJoinKey &other) const {
    return column_value_.CompareEquals(other.column_value_) == CmpBool::CmpTrue;
  }
};

/** HashJoinValue represents a value of an hash table entry */
struct HashJoinValue {
  /** The values are full tuples with the same hash key */
  std::vector<Tuple> tuples_;
};

}  // namespace bustub

namespace std {

/** Implements std::hash on HashJoinKey */
template <>
struct hash<bustub::HashJoinKey> {
  std::size_t operator()(const bustub::HashJoinKey &hash_key) const {
    size_t curr_hash = 0;
    if (!hash_key.column_value_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&hash_key.column_value_));
    }
    return curr_hash;
  }
};

}  // namespace std
