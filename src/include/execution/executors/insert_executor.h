//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor)
      : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {}

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    table_meta_data_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple) override {
    if (plan_->IsRawInsert()) {
      for (size_t i = 0; i < plan_->RawValues().size(); i++) {
        auto t = Tuple(plan_->RawValuesAt(i),&table_meta_data_->schema_);
        RID rid;
        if (!table_meta_data_->table_->InsertTuple(t,&rid,exec_ctx_->GetTransaction())) return false;
      }
      return true;
    } else {
      child_executor_->Init();
      Tuple t;
      while (child_executor_->Next(&t)) {
        RID rid;
        if (!table_meta_data_->table_->InsertTuple(t,&rid,exec_ctx_->GetTransaction())) return false;
      }
      return true;
    }
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;
  TableMetadata* table_meta_data_;
  std::unique_ptr<AbstractExecutor> child_executor_;
};
}  // namespace bustub
