//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx),plan_(plan) {}

  void Init() override {
    auto output_column_cnt = GetOutputSchema()->GetColumnCount();
    exprs_.resize(GetOutputSchema()->GetColumnCount());
    // get all the exprs_ that evaluate to the result
    for (size_t i = 0; i< output_column_cnt; i++) {
      exprs_[i] = GetOutputSchema()->GetColumn(i).GetExpr();
    }
    // retrieve first page and build table iterator
    table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    RID rid;
    auto page = static_cast<TablePage*> (exec_ctx_->GetBufferPoolManager()->FetchPage(table_meta_data_->table_->GetFirstPageId()));
    page->RLatch();
    page->GetFirstTupleRid(&rid);
    page->RUnlatch();
    exec_ctx_->GetBufferPoolManager()->UnpinPage(page->GetPageId(),false);
    iter_ = std::make_unique<TableIterator>(table_meta_data_->table_.get(),rid,exec_ctx_->GetTransaction());
  }

  bool Next(Tuple *tuple) override {
    while (*iter_ != table_meta_data_->table_->End()) {
      // no predicate or find the next tuple satisfying the predicate
      if (plan_->GetPredicate() ==
          nullptr || plan_->GetPredicate()->Evaluate(&**iter_, &table_meta_data_->schema_).GetAs<bool>()) break;
      ++(*iter_);
    }
    if (*iter_ == table_meta_data_->table_->End()) return false;
    vector<Value> values(GetOutputSchema()->GetColumnCount());
    // evaluate to the values that the final result want
    for (size_t i = 0; i < values.size(); i++) {
      values[i] = exprs_[i]->Evaluate(&**iter_,&table_meta_data_->schema_);
    }
    *tuple = Tuple(values,GetOutputSchema());
    ++(*iter_);
    return true;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  std::unique_ptr<TableIterator> iter_;
  TableMetadata* table_meta_data_{};
  vector<const AbstractExpression *> exprs_;
};
}  // namespace bustub
