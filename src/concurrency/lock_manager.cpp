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

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // Check whether the current row has a lock request queue in the lock table
  // i.e. check whether the current request is the first request
  // lock the lock manager to ensure each time only one tranction can
  // be granted/revoked a lock
  std::unique_lock<std::mutex> lock(latch_);
  auto &request_queue = lock_table_[rid];
  // Check some edge cases
  if (txn->GetState() == TransactionState::ABORTED) {
    // If the current transaction is aborted
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    // According to 2PL, if the current transaction is not in
    // the growing stage, it can not acquire any locks
    // The txn is thus aborted and throws and exception
    txn->SetState(TransactionState::ABORTED);
    ClearLock(&request_queue, txn, rid);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid) || txn->IsSharedLocked(rid)) {
    // If the current transaction has been locked on an already locked RID
    // the behavior is undefined
    // We choose to do nothing and return true, pretending the lock is acquired successfully
    return true;
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    ClearLock(&request_queue, txn, rid);
    return false;
  }
  // Kill all young transactions to perform wound-wait
  // The flag is necessary to nofify other waiting threads
  bool flag = false;
  // Insert the request into the queue
  InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::SHARED, false);
  for (auto request_iter = request_queue.request_queue_.begin(); request_iter != request_queue.request_queue_.end();) {
    // Finish scanning all the requests before the current one
    if (request_iter->txn_id_ == txn->GetTransactionId() && request_iter->lock_mode_ == LockMode::SHARED) {
      break;
    }
    if (request_iter->lock_mode_ == LockMode::EXCLUSIVE && request_iter->txn_id_ > txn->GetTransactionId()) {
      TransactionManager::GetTransaction(request_iter->txn_id_)->GetExclusiveLockSet()->erase(rid);
      TransactionManager::GetTransaction(request_iter->txn_id_)->SetState(TransactionState::ABORTED);
      request_iter = request_queue.request_queue_.erase(request_iter);
      flag = true;
    } else {
      request_iter++;
    }
  }
  if (flag) {
    request_queue.cv_.notify_all();
  }
  // Wait until the lock can be granted
  while (txn->GetState() != TransactionState::ABORTED && !ValidSharedLock(&request_queue, txn)) {
    request_queue.cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // The locking condition is satisfied, grant the lock
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // Check whether the current row has a lock request queue in the lock table
  // i.e. check whether the current request is the first request
  // lock the lock manager to ensure each time only one tranction can
  // be granted/revoked a lock
  std::unique_lock<std::mutex> lock(latch_);
  auto &request_queue = lock_table_[rid];
  // Check some edge cases
  if (txn->GetState() == TransactionState::ABORTED) {
    // If the current transaction is aborted
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    // According to 2PL, if the current transaction is not in
    // the growing stage, it can not acquire any locks
    // The txn is thus aborted and throws and exception
    txn->SetState(TransactionState::ABORTED);
    ClearLock(&request_queue, txn, rid);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    // If the current transaction has been locked on an already locked RID
    // the behavior is undefined
    // We choose to do nothing and return true, pretending the lock is acquired successfully
    return true;
  }
  // Kill all young transactions to perform wound-wait
  // The flag is necessary to nofify other waiting threads
  bool flag = false;
  // Insert the request into the queue
  InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
  for (auto request_iter = request_queue.request_queue_.begin(); request_iter != request_queue.request_queue_.end();) {
    // Finish scanning all the requests before the current one
    if (request_iter->txn_id_ == txn->GetTransactionId() && request_iter->lock_mode_ == LockMode::EXCLUSIVE) {
      break;
    }
    if (request_iter->txn_id_ > txn->GetTransactionId()) {
      if (request_iter->lock_mode_ == LockMode::SHARED) {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetSharedLockSet()->erase(rid);
      } else {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetExclusiveLockSet()->erase(rid);
      }
      TransactionManager::GetTransaction(request_iter->txn_id_)->SetState(TransactionState::ABORTED);
      request_iter = request_queue.request_queue_.erase(request_iter);
      flag = true;
    } else {
      request_iter++;
    }
  }
  if (flag) {
    request_queue.cv_.notify_all();
  }
  // Wait until the lock can be granted
  while (txn->GetState() != TransactionState::ABORTED && !ValidExclusiveLock(&request_queue, txn)) {
    request_queue.cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // The locking condition is satisfied, grant the lock
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &request_queue = lock_table_[rid];
  // Check some edge cases
  if (txn->GetState() == TransactionState::ABORTED) {
    // If the current transaction is aborted
    // Do nothing and return false, lock fails
    return false;
  }
  if (txn->GetState() == TransactionState::SHRINKING) {
    // According to 2PL, if the current transaction is not in
    // the growing stage, it can not upgrade any locks
    // The txn is thus aborted and throws and exception
    txn->SetState(TransactionState::ABORTED);
    ClearLock(&request_queue, txn, rid);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    // If the current transaction already holds an exclusive lock
    // Do not need to upgrade it, do nothing and return true
    return true;
  }
  if (!txn->IsSharedLocked(rid)) {
    // If the transaction does not hold a shared lock at the moment, return false
    // This means that the transaction does not hold any lock
    return false;
  }
  // Upgrade the lock
  if (request_queue.upgrading_ != INVALID_TXN_ID) {
    // If another transaction is already waiting to upgrade their lock
    // Abort and return false
    txn->SetState(TransactionState::ABORTED);
    ClearLock(&request_queue, txn, rid);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  request_queue.upgrading_ = txn->GetTransactionId();
  // Kill all young transactions to perform wound-wait
  // The flag is necessary to nofify other waiting threads
  bool flag = false;
  // Insert the request into the queue
  InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
  for (auto request_iter = request_queue.request_queue_.begin(); request_iter != request_queue.request_queue_.end();) {
    // Finish scanning all the requests before the current one
    if (request_iter->txn_id_ == txn->GetTransactionId() && request_iter->lock_mode_ == LockMode::SHARED) {
      if (request_iter->lock_mode_ == LockMode::SHARED) {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetSharedLockSet()->erase(rid);
      } else {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetExclusiveLockSet()->erase(rid);
      }
      request_iter = request_queue.request_queue_.erase(request_iter);
      flag = true;
    } else if (request_iter->txn_id_ > txn->GetTransactionId()) {
      if (request_iter->lock_mode_ == LockMode::SHARED) {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetSharedLockSet()->erase(rid);
      } else {
        TransactionManager::GetTransaction(request_iter->txn_id_)->GetExclusiveLockSet()->erase(rid);
      }
      TransactionManager::GetTransaction(request_iter->txn_id_)->SetState(TransactionState::ABORTED);
      request_iter = request_queue.request_queue_.erase(request_iter);
      flag = true;
    } else {
      request_iter++;
    }
  }
  if (flag) {
    request_queue.cv_.notify_all();
  }
  // Wait until the lock can be granted, should wait until the current request is in front of the queue
  while (txn->GetState() != TransactionState::ABORTED && !ValidExclusiveLock(&request_queue, txn)) {
    request_queue.cv_.wait(lock);
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    request_queue.upgrading_ = INVALID_TXN_ID;
    return false;
  }
  // The locking condition is satisfied, upgrade the lock
  // Upgrade the lock, change the mode
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  request_queue.upgrading_ = INVALID_TXN_ID;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
  auto &request_queue = lock_table_[rid];
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ && txn->GetState() == TransactionState::GROWING) {
    // In 2PL, if we try unlocking, then we enter the shrinking stage
    txn->SetState(TransactionState::SHRINKING);
  }
  // Get the request queue on rid, guaranteed existing
  // Find the lock held by txn in the queue and release it
  for (auto lock_request_temp = request_queue.request_queue_.begin();
       lock_request_temp != request_queue.request_queue_.end();) {
    if (lock_request_temp->txn_id_ == txn->GetTransactionId()) {
      // Remove the lock from the transaction shared / exclusive sets
      // notify all waiting threads in this queue
      ClearLock(&request_queue, txn, rid);
      request_queue.cv_.notify_all();
      // Assume there only one lock for each transaction per rid
      // Return immediately
      return true;
    }
    ++lock_request_temp;
  }
  return false;
}

void LockManager::InsertIntoRequestQueue(LockRequestQueue *request_queue, txn_id_t txn_id, LockMode lock_mode,
                                         bool granted) {
  for (auto &request_iter : request_queue->request_queue_) {
    if (request_iter.txn_id_ == txn_id && request_iter.lock_mode_ == lock_mode) {
      // The request already exists
      // Do nothing
      request_iter.granted_ = granted;
      return;
    }
  }
  LockRequest request = LockRequest(txn_id, lock_mode);
  request.granted_ = granted;
  request_queue->request_queue_.emplace_back(request);
}

void LockManager::ClearLock(LockRequestQueue *request_queue, Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->erase(rid);
  txn->GetSharedLockSet()->erase(rid);
  if (request_queue == nullptr) {
    return;
  }
  for (auto request_iter = request_queue->request_queue_.begin();
       request_iter != request_queue->request_queue_.end();) {
    if (request_iter->txn_id_ == txn->GetTransactionId()) {
      // Clear transaction's lock set
      request_iter = request_queue->request_queue_.erase(request_iter);
      return;
    }
    ++request_iter;
  }
}

bool LockManager::ValidSharedLock(LockRequestQueue *request_queue, Transaction *txn) {
  for (auto &iter : request_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId() && iter.lock_mode_ == LockMode::SHARED) {
      return true;
    }
    if (iter.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  return true;
}

bool LockManager::ValidExclusiveLock(LockRequestQueue *request_queue, Transaction *txn) {
  return request_queue->request_queue_.front().txn_id_ == txn->GetTransactionId() &&
         request_queue->request_queue_.front().lock_mode_ == LockMode::EXCLUSIVE;
}

}  // namespace bustub
