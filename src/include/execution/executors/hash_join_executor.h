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
    if (index_ == tmp_tuples_.size()) {
      // all the buffered result tuples has been traversed
      // fetch new ones
      tmp_tuples_.clear();
      index_ = 0;
      Tuple right_tuple;
      for (auto page_id : tmp_page_ids_) {
        // delete all the tmp page created by the last right tuple
        exec_ctx_->GetBufferPoolManager()->DeletePage(page_id);
      }
      tmp_page_ids_.clear();
      if (!right_->Next(&right_tuple)) return false;
      // get corresponding left tuples
      vector<Tuple> tuples;
      auto value = right_expr_->Evaluate(&right_tuple, right_->GetOutputSchema());
      jht_.GetValue(exec_ctx_->GetTransaction(), value, &tuples);
      if (tuples.size() == 1) {
        // if we only find one left tuple, we don't need to create a tmp page to buffered the result
        auto left_tuple = tuples[0];
        if (IsValidCombination(left_tuple, right_tuple)) {
          MakeOutputTuple(tuple, &left_tuple, &right_tuple);
          return true;
        }
      } else {
        TmpTuplePage *tmp_page = nullptr;
        page_id_t tmp_page_id = INVALID_PAGE_ID;
        // buffer all the valid output tuple in tmp pages
        for (const auto &left_tuple : tuples) {
          if (IsValidCombination(left_tuple, right_tuple)) {
            Tuple out_tuple;
            MakeOutputTuple(&out_tuple, &left_tuple, &right_tuple);
            TmpTuple tmp_tuple;
            if (!tmp_page || !tmp_page->Insert(out_tuple, &tmp_tuple)) {
              // create new tmp page
              tmp_page = reinterpret_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->NewPage(&tmp_page_id));
              if (!tmp_page) throw std::runtime_error("fail to create new tmp page when doing hash join");
              tmp_page->Init(tmp_page_id, PAGE_SIZE);
              tmp_page_ids_.push_back(tmp_page_id);
              assert(tmp_page->Insert(out_tuple, &tmp_tuple));
            }
            tmp_tuples_.push_back(tmp_tuple);
          }
        }
      }
    }
    // there is buffered tuples
    // just return the next buffered tuple
    auto tmp_tuple = tmp_tuples_[index_];
    index_++;
    auto page_id = tmp_tuple.GetPageId();
    if (last_page_id_ != INVALID_PAGE_ID && page_id != last_page_id_) {
      exec_ctx_->GetBufferPoolManager()->UnpinPage(last_page_id_, false);
    }
    last_page_id_ = page_id;
    auto tmp_tuple_page = reinterpret_cast<TmpTuplePage *>(exec_ctx_->GetBufferPoolManager()->FetchPage(page_id));
    if (!tmp_tuple_page) throw std::runtime_error("fail to fetch buffered tmp page when doing hash join");
    tmp_tuple_page->Get(tuple, tmp_tuple.GetOffset());
    if (index_ == tmp_tuples_.size()) {
      // this is the last tmp tuple in buffer
      // unpin its page
      exec_ctx_->GetBufferPoolManager()->UnpinPage(page_id, false);
      last_page_id_ = INVALID_PAGE_ID;
    }
    return true;
  }

  bool IsValidCombination(const Tuple &left_tuple, const Tuple &right_tuple) {
    return plan_->Predicate()
        ->EvaluateJoin(&left_tuple, left_->GetOutputSchema(), &right_tuple, right_->GetOutputSchema())
        .GetAs<bool>();
  }

  void MakeOutputTuple(Tuple *output_tuple, const Tuple *left_tuple, const Tuple *right_tuple) {
    auto output_column_cnt = GetOutputSchema()->GetColumnCount();
    vector<Value> values(output_column_cnt);
    for (size_t i = 0; i < output_column_cnt; i++) {
      values[i] =
          output_exprs_[i]->EvaluateJoin(left_tuple, left_->GetOutputSchema(), right_tuple, right_->GetOutputSchema());
    }
    *output_tuple = Tuple(values, GetOutputSchema());
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
