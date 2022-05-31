//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/**
 * A simple hash table that has all the necessary functionality for hash distinct.
 */
class HashDistinctTable {
 public:
  /**
   * Construct a new HashJoinTable instance.
   */
  HashDistinctTable() = default;

  /**
   * Insert a key-value pair into the hash table
   * @param hash_key the key to be inserted
   * @param hash_value the value to be inserted
   */
  void Insert(const HashDistinctKey &hash_key, const HashDistinctValue &hash_value) {
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
  bool HasKey(const HashDistinctKey &hash_key) { return !(hash_table_.count(hash_key) == 0); }

  /**
   * Get the value given a key
   */
  const HashDistinctValue &GetValue(const HashDistinctKey &hash_key) {
    if (!HasKey(hash_key)) {
      throw Exception(ExceptionType::INVALID, "Hash key does not exist.");
    }
    return hash_table_[hash_key];
  }

  /** An iterator over the hash join table */
  class Iterator {
   public:
    /** Create an iterator for the hash map */
    explicit Iterator(std::unordered_map<HashDistinctKey, HashDistinctValue>::const_iterator iter) : iter_{iter} {}

    /** @return The key of the iterator */
    const HashDistinctKey &Key() { return iter_->first; }

    /** @return The value of the iterator */
    const HashDistinctValue &Value() { return iter_->second; }

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
    std::unordered_map<HashDistinctKey, HashDistinctValue>::const_iterator iter_;
  };

  /** @return Iterator to the start of the hash table */
  Iterator Begin() { return Iterator{hash_table_.cbegin()}; }

  /** @return Iterator to the end of the hash table */
  Iterator End() { return Iterator{hash_table_.cend()}; }

 private:
  /** The hash table is a map from keys to values */
  std::unordered_map<HashDistinctKey, HashDistinctValue> hash_table_{};
};

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** @return The tuple as an HashDistinctKey */
  HashDistinctKey MakeHashDistinctKey(const Tuple *tuple) {
    std::vector<Value> keys;
    for (size_t col_idx = 0; col_idx < GetOutputSchema()->GetColumnCount(); col_idx++) {
      keys.emplace_back(tuple->GetValue(GetOutputSchema(), col_idx));
    }
    return {keys};
  }

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** The built hash table */
  HashDistinctTable hash_table_;
  // The iterator is not required because the operation can be pipelined
  // HashJoinTable::Iterator hash_table_iter_;
};
}  // namespace bustub
