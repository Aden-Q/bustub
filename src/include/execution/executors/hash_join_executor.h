//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
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

namespace bustub {
/**
 * A simple hash table that has all the necessary functionality for hash join.
 */
class HashJoinTable {
 public:
  /**
   * Construct a new HashJoinTable instance.
   */
  HashJoinTable() = default;

  /**
   * Insert a key-value pair into the hash table
   * @param hash_key the key to be inserted
   * @param hash_value the value to be inserted
   */
  void Insert(const HashJoinKey &hash_key, const HashJoinValue &hash_value) {
    if (HasKey(hash_key)) {
      // If the key already exists, append the new value to the end of the old one
      hash_table_[hash_key].tuples_.insert(hash_table_[hash_key].tuples_.end(), hash_value.tuples_.begin(),
                                           hash_value.tuples_.end());
    } else {
      // The key does not exist
      hash_table_.insert(std::make_pair(hash_key, hash_value));
    }
  }

  /**
   * Check whether a key exists in the hash table
   * @param hash_key the key to be checked
   */
  bool HasKey(const HashJoinKey &hash_key) { return !(hash_table_.count(hash_key) == 0); }

  /**
   * Get the value given a key
   */
  const HashJoinValue &GetValue(const HashJoinKey &hash_key) {
    if (!HasKey(hash_key)) {
      throw Exception(ExceptionType::INVALID, "Hash key does not exist.");
    }
    return hash_table_[hash_key];
  }

  /** An iterator over the hash join table */
  class Iterator {
   public:
    /** Create an iterator for the hash join map */
    explicit Iterator(std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    const HashJoinKey &Key() { return iter_->first; }

    /** @return The value of the iterator */
    const HashJoinValue &Value() { return iter_->second; }

    /** @return The iterator before it is incremented */
    Iterator &operator++() {
      ++iter_;
      return *this;
    }

    /** @return `true` if both iterators are identical */
    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

    /** @return `true` if both iterators are different */
    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

   private:
    /** Hash join map */
    std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  Iterator Begin() { return Iterator{hash_table_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  Iterator End() { return Iterator{hash_table_.cend()}; }

 private:
  /** The hash table is a map from keys to values */
  std::unordered_map<HashJoinKey, HashJoinValue> hash_table_{};
};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  /** The child executor that produces tuple for the left side of join */
  std::unique_ptr<AbstractExecutor> left_child_;
  /** The child executor that produces tuple for the right side of join */
  std::unique_ptr<AbstractExecutor> right_child_;
  /** The built hash table */
  HashJoinTable hash_table_;
  // The iterator is not required because the join phase is not a sequential scan
  // HashJoinTable::Iterator hash_table_iter_;
  /** Store join results */
  std::vector<Tuple> join_results_;
  /** */
  std::vector<Tuple>::iterator join_results_iter_;
};

}  // namespace bustub
