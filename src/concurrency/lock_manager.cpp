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
share_start:
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
    // request_queue.cv_.notify_all();
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
    // request_queue.cv_.notify_all();
    return false;
  }
  // Use strict 2PL
  // Check the request queue for the current RID
  for (auto lock_request_temp = request_queue.request_queue_.begin();
       lock_request_temp != request_queue.request_queue_.end();) {
    if (lock_request_temp->lock_mode_ == LockMode::EXCLUSIVE) {
      // compare the priority between transacitons
      if (lock_request_temp->txn_id_ > txn->GetTransactionId()) {
        // In the case that the current transaction is older
        // Abort the holding transaction and release the lock
        TransactionManager::GetTransaction(lock_request_temp->txn_id_)->SetState(TransactionState::ABORTED);
        // throw TransactionAbortException(lock_request_temp->txn_id_, AbortReason::DEADLOCK);
        TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetExclusiveLockSet()->erase(rid);
        TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetSharedLockSet()->erase(rid);
        lock_request_temp = request_queue.request_queue_.erase(lock_request_temp);
        // request_queue.cv_.notify_all();
      } else if (lock_request_temp->txn_id_ < txn->GetTransactionId()) {
        // If there are exclusive locks in the request queue, held by older transactions, then wait
        InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::SHARED, false);
        txn->GetSharedLockSet()->emplace(rid);
        request_queue.cv_.wait(lock);
        goto share_start;
      }
    } else {
      ++lock_request_temp;
    }
  }
  InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::SHARED, true);
  // Acquire the lock successfully
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // Check whether the current row has a lock request queue in the lock table
  // i.e. check whether the current request is the first request
  // lock the lock manager to ensure each time only one tranction can
  // be granted/revoked a lock
  std::unique_lock<std::mutex> lock(latch_);
exclusive_start:
  auto &request_queue = lock_table_[rid];
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
    ClearLock(&request_queue, txn, rid);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    // request_queue.cv_.notify_all();
    return false;
  }
  if (txn->IsExclusiveLocked(rid)) {
    // If the current transaction has been locked on an already locked RID
    // the behavior is undefined
    // We choose to do nothing and return true, pretending the lock is acquired successfully
    return true;
  }
  // Use 2PL
  // Check the request queue for the current RID
  for (auto lock_request_temp = request_queue.request_queue_.begin();
       lock_request_temp != request_queue.request_queue_.end();) {
    // compare the priority between transacitons
    if (lock_request_temp->txn_id_ > txn->GetTransactionId()) {
      // In the case that the current transaction is older
      // Abort the holding transaction and release the lock
      // Remove locks
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->SetState(TransactionState::ABORTED);
      // throw TransactionAbortException(lock_request_temp->txn_id_, AbortReason::DEADLOCK);
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetExclusiveLockSet()->erase(rid);
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetSharedLockSet()->erase(rid);
      lock_request_temp = request_queue.request_queue_.erase(lock_request_temp);
      // request_queue.cv_.notify_all();
    } else if (lock_request_temp->txn_id_ < txn->GetTransactionId()) {
      InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, false);
      txn->GetExclusiveLockSet()->emplace(rid);
      // If there are exclusive locks in the request queue, held by older transactions, then wait


      request_queue.cv_.wait(lock);
      goto exclusive_start;
    } else {
      ++lock_request_temp;
    }
  }
  InsertIntoRequestQueue(&request_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE, true);
  // Acquire the lock successfully
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(latch_);
upgrade_start:
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
    // request_queue->second.cv_.notify_all();
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
    // request_queue.cv_.notify_all();
    return false;
  }
  request_queue.upgrading_ = txn->GetTransactionId();
  // Find the lock held by txn first
  // Check the request queue for the current RID
  for (auto lock_request_temp = request_queue.request_queue_.begin();
       lock_request_temp != request_queue.request_queue_.end();) {
    // compare the priority between transacitons
    if (lock_request_temp->txn_id_ > txn->GetTransactionId()) {
      // In the case that the current transaction is older
      // Abort the holding transaction and release the lock
      // Remove locks
      // throw TransactionAbortException(lock_request_temp->txn_id_, AbortReason::DEADLOCK);
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->SetState(TransactionState::ABORTED);
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetExclusiveLockSet()->erase(rid);
      TransactionManager::GetTransaction(lock_request_temp->txn_id_)->GetSharedLockSet()->erase(rid);
      lock_request_temp = request_queue.request_queue_.erase(lock_request_temp);
      // request_queue.cv_.notify_all();
    } else if (lock_request_temp->txn_id_ < txn->GetTransactionId()) {
      request_queue.cv_.wait(lock);
      // After being waken up, need to recheck because it is possibly that some request is removed
      // from the queue
      goto upgrade_start;
    } else {
      ++lock_request_temp;
    }
  }
  // At the end of the loop
  // All newer transactions have been aborted
  // All older transactions have released the locks
  // The request queue has size 1, containing only the shared lock by txn
  BUSTUB_ASSERT(request_queue.request_queue_.size() == 1, "There are still other locks in the queue.");
  auto &lock_request = request_queue.request_queue_.front();
  BUSTUB_ASSERT(lock_request.lock_mode_ == LockMode::SHARED, "The lock is not in shared mode.");
  // Upgrade the lock, change the mode
  lock_request.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  // Upgrade successfully
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
    if (request_iter.txn_id_ == txn_id) {
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
    }
    ++request_iter;
  }
}

bool LockManager::ValidSharedLock(LockRequestQueue *request_queue, Transaction *txn) {
  for (auto &iter : request_queue->request_queue_) {
    if (iter.txn_id_ == txn->GetTransactionId()) {
      return true;
    } else if (iter.lock_mode_ == LockMode::EXCLUSIVE) {
      return false;
    }
  }
  return true;
}

}  // namespace bustub
