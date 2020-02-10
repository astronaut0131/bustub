//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
// using HT = SimpleHashJoinHashTable;

using HashJoinKeyType = Value;
using HashJoinValType = Tuple;
using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, ValueComparator>;

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx),
        plan_(plan),
        jht_("hash join", exec_ctx_->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_),
        left_(std::move(left)),
        right_(std::move(right)) {}

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    // get exprs to evaluate to output result
    auto output_column_cnt = GetOutputSchema()->GetColumnCount();
    output_exprs_.resize(output_column_cnt);
    for (size_t i = 0; i < output_column_cnt; i++) {
      output_exprs_[i] = GetOutputSchema()->GetColumn(i).GetExpr();
    }
    // build hash table from left
    left_->Init();
    right_->Init();
    left_expr_ = plan_->Predicate()->GetChildAt(0);
    right_expr_ = plan_->Predicate()->GetChildAt(1);
    Tuple tuple;
    while (left_->Next(&tuple)) {
      Value value = left_expr_->Evaluate(&tuple, left_->GetOutputSchema());
      jht_.Insert(exec_ctx_->GetTransaction(), value, tuple);
    }
  }

  bool Next(Tuple *tuple) override {
    vector<const AbstractExpression *> exprs{right_expr_};
    // tuple from right
    Tuple right_tuple;
    size_t output_column_cnt = output_exprs_.size();
    while (true) {
      while (index_ == tmp_tuples_.size()) {
        // deallocate all the tmp pages created by the last input right tuple
        for (auto tmp_page_id : tmp_page_ids_) {
          exec_ctx_->GetBufferPoolManager()->DeletePage(tmp_page_id);
        }
        tmp_page_ids_.clear();
        // all the left tuples corresponding to the input right tuple has exhausted
        // move to the next right tuple
        index_ = 0;
        tmp_tuples_.clear();
        // all right tuples have been traversed
        if (!right_->Next(&right_tuple)) return false;
        vector<Tuple> tuples;
        auto value = right_expr_->Evaluate(&right_tuple, right_->GetOutputSchema());
        jht_.GetValue(exec_ctx_->GetTransaction(), value, &tuples);
        if (!tuples.empty()) {
          tmp_tuples_.resize(tuples.size());
          page_id_t new_page_id;
          auto tmp_page = static_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->NewPage(&new_page_id));
          if (!tmp_page) throw std::runtime_error("fail to allocate temp page when doing hash join");
          tmp_page_ids_.push_back(new_page_id);
          tmp_page->Init(new_page_id, PAGE_SIZE);
          for (size_t i = 0; i < tuples.size(); i++) {
            if (!tmp_page->Insert(tuples[i], &tmp_tuples_[i])) {
              exec_ctx_->GetBufferPoolManager()->UnpinPage(new_page_id, true);
              tmp_page = static_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->NewPage(&new_page_id));
              if (!tmp_page) throw std::runtime_error("fail to allocate temp page when doing hash join");
              tmp_page_ids_.push_back(new_page_id);
              tmp_page->Init(new_page_id, PAGE_SIZE);
              // reinsert tuples[i] to the newly created page
              i--;
            }
          }
        }
      }
      TmpTuple tmp_tuple;
      Tuple left_tuple;
      while (index_ < tmp_tuples_.size()) {
        tmp_tuple = tmp_tuples_[index_];
        FetchTuple(&left_tuple, exec_ctx_->GetBufferPoolManager(), tmp_tuple, last_page_id_);
        if (plan_->Predicate()
                ->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple, right_->GetOutputSchema())
                .GetAs<bool>())
          break;
        index_++;
      }
      if (index_ < tmp_tuples_.size()) {
        vector<Value> values(output_column_cnt);
        for (size_t i = 0; i < output_column_cnt; i++) {
          values[i] = output_exprs_[i]->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple,
                                                     right_->GetOutputSchema());
        }
        *tuple = Tuple(values, GetOutputSchema());
        index_++;
        return true;
      } else {
        exec_ctx_->GetBufferPoolManager()->UnpinPage(last_page_id_, false);
      }
    }
  }

  // helper function
  static void FetchTuple(Tuple *tuple, BufferPoolManager *bpm, TmpTuple tmp_tuple, page_id_t &last_page_id) {
    page_id_t page_id = tmp_tuple.GetPageId();
    if (last_page_id != INVALID_PAGE_ID && page_id != last_page_id) {
      bpm->UnpinPage(last_page_id, false);
    }
    last_page_id = page_id;
    auto tmp_tuple_page = static_cast<TmpTuplePage *>(bpm->FetchPage(page_id));
    if (!tmp_tuple_page) throw std::runtime_error("fail to fetch page when doing hash join");
    tmp_tuple_page->Get(tuple, tmp_tuple.GetOffset());
  }
  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  /** The hash join plan node. */
  const HashJoinPlanNode *plan_;
  /** The comparator is used to compare hashes. */
  ValueComparator jht_comp_{};
  /** The identity hash function. */
  HashFunction<Value> jht_hash_fn_{};

  /** The hash table that we are using. */
  HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;
  std::unique_ptr<AbstractExecutor> left_;
  std::unique_ptr<AbstractExecutor> right_;
  const AbstractExpression *left_expr_;
  const AbstractExpression *right_expr_;
  vector<TmpTuple> tmp_tuples_;
  size_t index_ = 0;
  vector<const AbstractExpression *> output_exprs_;
  vector<page_id_t> tmp_page_ids_;
  page_id_t last_page_id_ = INVALID_PAGE_ID;
};
}  // namespace bustub
