/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

#include "query_execution/PolicyEnforcerSingleNode.hpp"

#include <cstddef>
#include <memory>
#include <queue>
#include <utility>
#include <unordered_map>
#include <vector>

#include "catalog/CatalogRelation.hpp"
#include "catalog/CatalogRelationSchema.hpp"
#include "catalog/CatalogTypedefs.hpp"
#include "query_execution/QueryExecutionState.hpp"
#include "query_execution/QueryManagerBase.hpp"
#include "query_execution/QueryManagerSingleNode.hpp"
#include "query_execution/WorkerDirectory.hpp"
#include "query_execution/WorkerMessage.hpp"
#include "query_execution/QueryExecutionMessages.pb.h"
#include "query_optimizer/QueryHandle.hpp"
#include "relational_operators/AggregationOperator.hpp"
#include "relational_operators/BuildHashOperator.hpp"
#include "relational_operators/BuildLIPFilterOperator.hpp"
#include "relational_operators/HashJoinOperator.hpp"
#include "relational_operators/NestedLoopsJoinOperator.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/SelectOperator.hpp"
#include "relational_operators/SortRunGenerationOperator.hpp"
#include "relational_operators/UnionAllOperator.hpp"
#include "relational_operators/BuildAggregationExistenceMapOperator.hpp"
#include "relational_operators/BuildHashOperator.hpp"
#include "relational_operators/CreateIndexOperator.hpp"
#include "relational_operators/DeleteOperator.hpp"
#include "relational_operators/DestroyAggregationStateOperator.hpp"
#include "relational_operators/FinalizeAggregationOperator.hpp"
#include "relational_operators/InitializeAggregationOperator.hpp"
#include "relational_operators/InsertOperator.hpp"
#include "relational_operators/NestedLoopsJoinOperator.hpp"
#include "relational_operators/SampleOperator.hpp"
#include "relational_operators/SelectOperator.hpp"
#include "relational_operators/SortMergeRunOperator.hpp"
#include "relational_operators/SortRunGenerationOperator.hpp"
#include "relational_operators/TableExportOperator.hpp"
#include "relational_operators/TableGeneratorOperator.hpp"
#include "relational_operators/UnionAllOperator.hpp"
#include "relational_operators/UpdateOperator.hpp"
#include "relational_operators/WindowAggregationOperator.hpp"

#include "gflags/gflags.h"
#include "glog/logging.h"

namespace quickstep {

DEFINE_uint64(max_msgs_per_dispatch_round, 20, "Maximum number of messages that"
              " can be allocated in a single round of dispatch of messages to"
              " the workers.");
DECLARE_bool(tenzin_profiling);

void PolicyEnforcerSingleNode::getWorkerMessages(
    std::vector<std::unique_ptr<WorkerMessage>> *worker_messages) {
  // Iterate over admitted queries until either there are no more
  // messages available, or the maximum number of messages have
  // been collected.
  DCHECK(worker_messages->empty());
  // TODO(harshad) - Make this function generic enough so that it
  // works well when multiple queries are getting executed.
  std::size_t per_query_share = 0;
  if (!admitted_queries_.empty()) {
    per_query_share = FLAGS_max_msgs_per_dispatch_round / admitted_queries_.size();
  } else {
    LOG(WARNING) << "Requesting WorkerMessages when no query is running";
    return;
  }
  DCHECK_GT(per_query_share, 0u);
  std::vector<std::size_t> finished_queries_ids;

  for (const auto &admitted_query_info : admitted_queries_) {
    QueryManagerBase *curr_query_manager = admitted_query_info.second.get();
    DCHECK(curr_query_manager != nullptr);
    std::size_t messages_collected_curr_query = 0;
    while (messages_collected_curr_query < per_query_share) {
      WorkerMessage *next_worker_message =
          static_cast<QueryManagerSingleNode*>(curr_query_manager)->getNextWorkerMessage(0, kAnyNUMANodeID);
      if (next_worker_message != nullptr) {
        ++messages_collected_curr_query;
        worker_messages->push_back(std::unique_ptr<WorkerMessage>(next_worker_message));
      } else {
        // No more work ordes from the current query at this time.
        // Check if the query's execution is over.
        if (curr_query_manager->getQueryExecutionState().hasQueryExecutionFinished()) {
          // If the query has been executed, remove it.
          finished_queries_ids.push_back(admitted_query_info.first);
        }
        break;
      }
    }
  }
  for (const std::size_t finished_qid : finished_queries_ids) {
    onQueryCompletion(admitted_queries_[finished_qid].get());
    removeQuery(finished_qid);
  }
}

bool PolicyEnforcerSingleNode::admitQuery(QueryHandle *query_handle) {
  if (admitted_queries_.size() < PolicyEnforcerBase::kMaxConcurrentQueries) {
    // Ok to admit the query.
    const std::size_t query_id = query_handle->query_id();
    if (admitted_queries_.find(query_id) == admitted_queries_.end()) {
      // Query with the same ID not present, ok to admit.
      admitted_queries_[query_id].reset(
          new QueryManagerSingleNode(foreman_client_id_, num_numa_nodes_, query_handle,
                                     catalog_database_, storage_manager_, bus_));
      return true;
    } else {
      LOG(ERROR) << "Query with the same ID " << query_id << " exists";
      return false;
    }
  } else {
    // This query will have to wait.
    waiting_queries_.push(query_handle);
    return false;
  }
}

std::string replaceQuote(std::string proto) {
  std::string ret = proto;
  for (size_t i = 0; i < ret.size(); ++i) {
    if (ret[i] == '\"') {
        ret.replace(i, 1, "\'");
    }
    if (ret[i] == '\\') {
        if (i+1 < ret.size()) {
          if (ret[i+1] == '0') {
            ret.replace(i, 1, " ");
          }
        }
    }
  }
  return ret;
}

void PolicyEnforcerSingleNode::onQueryCompletion(QueryManagerBase *query_manager) {
  const QueryHandle *query_handle = query_manager->query_handle();
  const std::size_t query_id = query_handle->query_id();

  if (FLAGS_tenzin_profiling && hasProfilingResults(query_id)) {
    std::ostringstream txt_filename;
    txt_filename << "record.json";

    const auto &dag = query_handle->getQueryPlan().getQueryPlanDAG();
    std::size_t num_nodes = dag.size();

    std::size_t overall_start_time = std::numeric_limits<std::size_t>::max();
    std::size_t overall_end_time = 0;
    std::size_t overall_memory_bytes = 0;

    std::vector<std::size_t> time_start(num_nodes, std::numeric_limits<std::size_t>::max());
    std::vector<std::size_t> time_end(num_nodes, 0);
    std::vector<std::size_t> time_elapsed(num_nodes, 0);
    std::vector<std::size_t> memory_bytes(num_nodes, 0);
    std::vector<std::size_t> num_work_orders(num_nodes, 0);

    FILE *fp = std::fopen(txt_filename.str().c_str(), "a");
    CHECK_NOTNULL(fp);
    std::ostringstream txt;

    // per work order
    std::vector<WorkOrderTimeEntry> profilingResults = getProfilingResults(query_id);
    for (const auto &entry : profilingResults) {
      txt << "{\"object\": \"work order\", \"part id\": "
          << entry.part_id
          << ", \"operator id\": "
          << entry.operator_id
          << ", \"query id\": "
          << query_id
          << ", \"rebuild\": "
          << (entry.rebuild ? "true" : "false")
          << ", \"start time\": "
          << entry.start_time
          << ", \"end time\": "
          << entry.end_time
          << ", \"time\": "
          << entry.end_time - entry.start_time
          << ", \"memory bytes\": "
          << entry.memory_bytes
          << ", \"work id\": "
          << entry.worker_id
          << ", \"proto\": \""
          << replaceQuote(entry.proto.ShortDebugString())
          << "\"}\n";

      const std::size_t workorder_start_time = entry.start_time;
      const std::size_t workorder_end_time = entry.end_time;
      const std::size_t relop_index = entry.operator_id;
      DCHECK_LT(relop_index, num_nodes);
      overall_start_time = std::min(overall_start_time, workorder_start_time);
      overall_end_time = std::max(overall_end_time, workorder_end_time);
      time_start[relop_index] =
        std::min(time_start[relop_index], workorder_start_time);
      time_end[relop_index] =
          std::max(time_end[relop_index], workorder_end_time);
      time_elapsed[relop_index] += (workorder_end_time - workorder_start_time);
      memory_bytes[relop_index] += entry.memory_bytes;
      overall_memory_bytes += entry.memory_bytes;
      num_work_orders[relop_index] += 1;
    }

    const std::vector<std::string>& input_relations = query_handle->getQueryPlan().getInputRelations();

    // per operator
    for (std::size_t node_index = 0; node_index < num_nodes; ++node_index) {
      const auto &node = dag.getNodePayload(node_index);
      // const RelationalOperator::OperatorType node_type = node.getOperatorType();

      std::string opName = node.getName();

      txt << "{\"object\": \"operator\", \"name\": \""
          << opName
          << "\", \"operator id\": "
          << node_index
          << ", \"query id\": "
          << query_id
          << ", \"start time\": "
          << time_start[node_index]
          << ", \"end time\": "
          << time_end[node_index]
          << ", \"overall time\": "
          << time_end[node_index] - time_start[node_index]
          << ", \"memory bytes\": "
          << memory_bytes[node_index]
          << ", \"total time elapsed\": "
          << time_elapsed[node_index]
          << ", \"num work orders\": "
          << num_work_orders[node_index];

      // insert destination
      QueryContext::insert_destination_id insert_dest_id = node.getInsertDestinationID();
      if (insert_dest_id != QueryContext::kInvalidInsertDestinationId) {
        txt << ", \"insert destination id\": " << insert_dest_id;
      }

      // output relation
      const relation_id rel_id = node.getOutputRelationID();
      if (rel_id != -1) {
        txt << ", \"output_relation_id\": " << rel_id;
      }

      // partition info
      txt << ", \"input_num_partitions\": "
          << node.getNumPartitions()
          << ", \"repartition\": "
          << (node.hasRepartition() ? "true" : "false")
          << ", \"output num partitions\": "
          << node.getOutputNumPartitions();

      // edges
      txt << ", \"edges\": [";
      bool first = true;
      for (const auto &link : dag.getDependents(node_index)) {

        if (first) {
          first = false;
        } else {
          txt << ", ";
        }

        txt << "{\"src node id\": "
            << node_index
            << ", \"dst node id\": "
            << link.first
            << ", \"is pipeline breaker\": "
            << (link.second ? "true" : "false")
            << "}";
      }
      txt << "]";

      txt << ", \"input relations\": ["
          << input_relations[node_index]
          << "]";

      const RelationalOperator::OperatorType node_type = node.getOperatorType();
      std::string attrs = "";
      switch (node_type) {
        case RelationalOperator::kAggregation: {
          const AggregationOperator &aggregation_op =
                    static_cast<const AggregationOperator&>(node);
          if (query_handle->getQueryContextProto().aggregation_states_size() > 0
                && aggregation_op.getAggregationId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().aggregation_states(
              aggregation_op.getAggregationId()).aggregation_state().ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kBuildHash: {
          const BuildHashOperator &build_hash_op =
              static_cast<const BuildHashOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && build_hash_op.getPredicateId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().predicates(
              build_hash_op.getPredicateId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kBuildLIPFilter: {
          const BuildLIPFilterOperator &build_lip_filter_op =
              static_cast<const BuildLIPFilterOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && build_lip_filter_op.getPredicateId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().predicates(
              build_lip_filter_op.getPredicateId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kDelete: {
          const DeleteOperator &delete_op =
              static_cast<const DeleteOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && delete_op.getPredicateId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().predicates(
              delete_op.getPredicateId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kDestroyAggregationState: {
          const DestroyAggregationStateOperator &destroy_agg_op =
                    static_cast<const DestroyAggregationStateOperator&>(node);
          if (query_handle->getQueryContextProto().aggregation_states_size() > 0
                && destroy_agg_op.getAggregationId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().aggregation_states(
              destroy_agg_op.getAggregationId()).aggregation_state().ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kFinalizeAggregation: {
          const FinalizeAggregationOperator &finalize_aggregation_op =
                    static_cast<const FinalizeAggregationOperator&>(node);
          if (query_handle->getQueryContextProto().aggregation_states_size() > 0
                && finalize_aggregation_op.getAggregationId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().aggregation_states(
                finalize_aggregation_op.getAggregationId()).aggregation_state().ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kInitializeAggregation: {
          const InitializeAggregationOperator &initialize_aggregation_op =
                    static_cast<const InitializeAggregationOperator&>(node);
          if (query_handle->getQueryContextProto().aggregation_states_size() > 0
                && initialize_aggregation_op.getAggregationId() > 0) {
            attrs = replaceQuote(query_handle->getQueryContextProto().aggregation_states(
                initialize_aggregation_op.getAggregationId()).aggregation_state().ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kNestedLoopsJoin: {
          const NestedLoopsJoinOperator &nlj_op =
              static_cast<const NestedLoopsJoinOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && nlj_op.getPredicateId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().predicates(
              nlj_op.getPredicateId()).ShortDebugString());
          }
          if (query_handle->getQueryContextProto().scalar_groups_size() > 0
                && nlj_op.getSelectionId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().scalar_groups(
              nlj_op.getSelectionId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kSelect: {
          const SelectOperator &select_op =
              static_cast<const SelectOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && select_op.getPredicateId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().predicates(
              select_op.getPredicateId()).ShortDebugString());
          }
          if (query_handle->getQueryContextProto().scalar_groups_size() > 0
                && select_op.getSelectionId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().scalar_groups(
              select_op.getSelectionId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kSortMergeRun: {
          const SortMergeRunOperator &sort_op =
              static_cast<const SortMergeRunOperator&>(node);
          if (query_handle->getQueryContextProto().sort_configs_size() > 0
                && sort_op.getSortConfig() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().sort_configs(
                sort_op.getSortConfig()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kSortRunGeneration: {
          const SortRunGenerationOperator &sort_run_op =
              static_cast<const SortRunGenerationOperator&>(node);
          if (query_handle->getQueryContextProto().sort_configs_size() > 0
                && sort_run_op.getSortConfig() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().sort_configs(
                sort_run_op.getSortConfig()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kUpdate: {
          const UpdateOperator &update_op =
              static_cast<const UpdateOperator&>(node);
          if (query_handle->getQueryContextProto().predicates_size() > 0
                && update_op.getPredicateId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().predicates(
              update_op.getPredicateId()).ShortDebugString());
          }
          if (query_handle->getQueryContextProto().update_groups_size() > 0
                && update_op.getUpdateId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().update_groups(
              update_op.getUpdateId()).ShortDebugString());
          }
          break;
        }
        case RelationalOperator::kWindowAggregation: {
          const WindowAggregationOperator &aggregation_op =
                    static_cast<const WindowAggregationOperator&>(node);
          if (query_handle->getQueryContextProto().window_aggregation_states_size() > 0
                && aggregation_op.getAggregationId() > 0) {
            attrs += replaceQuote(query_handle->getQueryContextProto().window_aggregation_states(
                aggregation_op.getAggregationId()).ShortDebugString());
          }
          break;
        }
        default:
          break;
      }

      txt << ", \"attribute proto\": \""
          << replaceQuote(attrs)
          << "\"";

      attrs = "";
      switch (node_type) {
        case RelationalOperator::kSelect: {
          const SelectOperator &select_op =
              static_cast<const SelectOperator&>(node);
          attrs = select_op.getSelectAttributes();
          break;
        }
        case RelationalOperator::kBuildAggregationExistenceMap: {
          const BuildAggregationExistenceMapOperator &build_agg_op =
              static_cast<const BuildAggregationExistenceMapOperator&>(node);
          const CatalogRelationSchema *input_relation =
            &build_agg_op.input_relation();
          attrs = "{\"relation\": " + std::to_string(input_relation->getID()) + \
            ", \"attribute\": " + std::to_string(build_agg_op.getAttribute()) + "}";
          break;
        }
        case RelationalOperator::kCreateIndex: {
          const CreateIndexOperator &create_index_op =
              static_cast<const CreateIndexOperator&>(node);
          attrs = create_index_op.getAttribute();
          break;
        }
        case RelationalOperator::kInnerJoin:
        case RelationalOperator::kLeftAntiJoin:
        case RelationalOperator::kLeftOuterJoin:
        case RelationalOperator::kLeftSemiJoin: {
          const HashJoinOperator &hash_join_op =
              static_cast<const HashJoinOperator&>(node);
          attrs = hash_join_op.getAttribute();
          break;
        }
        case RelationalOperator::kInsert: {
          const InsertOperator &insert_op =
                    static_cast<const InsertOperator&>(node);
          attrs = insert_op.getAttribute();
          break;
        }
        case RelationalOperator::kSample: {
          const SampleOperator &sample_op =
                    static_cast<const SampleOperator&>(node);
          attrs = sample_op.getAttribute();
          break;
        }
        case RelationalOperator::kTableExport: {
          const TableExportOperator &table_export_op =
              static_cast<const TableExportOperator&>(node);
          attrs = table_export_op.getAttribute();
          break;
        }
        case RelationalOperator::kTableGenerator: {
          const TableGeneratorOperator &table_generator_op =
              static_cast<const TableGeneratorOperator&>(node);
          attrs = table_generator_op.getAttribute();
          break;
        }
        case RelationalOperator::kUnionAll: {
          const UnionAllOperator &union_all_op =
            static_cast<const UnionAllOperator&>(node);
          attrs = union_all_op.getAttribute();
          break;
        }
        default:
          break;
      }

      txt << ", \"attribute list\": ["
          << attrs
          << "]";


      txt << "}\n";
    }

    // query overall
    double total_time_elapsed = 0;
    for (std::size_t i = 0; i < time_elapsed.size(); ++i) {
      total_time_elapsed += time_elapsed[i];
    }

    txt << "{\"object\": \"query\", \"query id\": "
        << query_id
        << ", \"start time\": "
        << overall_start_time
        << ", \"end time\": "
        << overall_end_time
        << ", \"overall time\": "
        << overall_end_time - overall_start_time
        << ", \"memory bytes\": "
        << overall_memory_bytes + query_manager->getQueryMemoryConsumptionBytes()
        << ", \"total time elapsed\": "
        << total_time_elapsed
        << ", \"query context proto\": \""
        << replaceQuote(query_handle->getQueryContextProto().ShortDebugString())
        << "\"}\n";

    const std::string txt_file_content = txt.str();
    const std::size_t txt_file_length = txt_file_content.length();

    CHECK_EQ(txt_file_length,
             std::fwrite(txt_file_content.c_str(), sizeof(char), txt_file_length, fp));

    std::fclose(fp);
  }
}

}  // namespace quickstep
