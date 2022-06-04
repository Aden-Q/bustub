//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // Check whether the current row has a lock request queue in the lock table
  // i.e. check whether the current request is the first request
  // lock the lock manager to ensure each time only one tranction can
  // be granted/revoked a lock
  std::unique_lock<std::mutex> lock(latch_);
  // Check some edge cases
  if (txn->GetState() == TransactionState::ABORTED) {
    // If the current transaction is aborted
    // Do nothing and return false, lock fails
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    // According to 2PL, if the current transaction is not in
    // the growing stage, it can not acquire any locks
    // The txn is thus aborted and throws and exception
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid) || txn->IsSharedLocked(rid)) {
    // If the current transaction has been locked on an already locked RID
    // the behavior is undefined
    // We choose to do nothing and return true, pretending the lock is acquired successfully
    return true;
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);
  auto request_queue = lock_table_.find(rid);
  if (request_queue == lock_table_.end()) {
    // If the current request is first lock request on this RID
    // Create and initialize a new request queue and append the request to it
    lock_table_.emplace(rid, LockRequestQueue());
    lock_table_[rid].request_queue_.emplace_back(lock_request);
    return true;
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    // TBD
  } else if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    // TBD
  } else if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    // Use strict 2PL
    // Check the request queue for the current RID
    for (auto lock_request_temp : request_queue->second.request_queue_) {
      if (lock_request_temp.lock_mode_ == LockMode::EXCLUSIVE) {
        // If there are exclusive locks in the request queue, wait
        request_queue->second.cv_.wait(lock);
        // After being waken up, need to recheck the state of the transaction
        // because it is possible that the current transaction is aborted by
        // other transactions, in that case, don't need to check other requests
        // in the queue anymore, return false instead
        if (txn->GetState() == TransactionState::ABORTED) {
          return false;
        }
      }
    }
    lock_request.granted_ = true;
    request_queue->second.request_queue_.emplace_back(lock_request);
  }
  // Acquire the lock successfully
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {



  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
