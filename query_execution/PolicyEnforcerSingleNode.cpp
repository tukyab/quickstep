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
  for (size_t i = 0; i < proto.size(); ++i) {
    if (proto[i] == '"') {
        proto.replace(i, 1, "\'");
    }
  }
  return proto;
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
          << ", \"proto\": \n\""
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

    // per operator
    for (std::size_t node_index = 0; node_index < num_nodes; ++node_index) {
      const auto &node = dag.getNodePayload(node_index);
      const RelationalOperator::OperatorType node_type = node.getOperatorType();

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

      // input relation
      txt << ", \"input relations\": [";
      const CatalogRelationSchema *input_relation = nullptr;
      std::string input_relation_info;
      switch (node_type) {
        case RelationalOperator::kAggregation: {
          const AggregationOperator &aggregation_op =
              static_cast<const AggregationOperator&>(node);
          input_relation = &aggregation_op.input_relation();
          input_relation_info = "input";
          break;
        }
        case RelationalOperator::kBuildHash: {
          const BuildHashOperator &build_hash_op =
              static_cast<const BuildHashOperator&>(node);
          input_relation = &build_hash_op.input_relation();
          input_relation_info = "input";
          break;
        }
        case RelationalOperator::kBuildLIPFilter: {
          const BuildLIPFilterOperator &build_lip_filter_op =
              static_cast<const BuildLIPFilterOperator&>(node);
          input_relation = &build_lip_filter_op.input_relation();
          input_relation_info = "input";
          break;
        }
        case RelationalOperator::kInnerJoin:
        case RelationalOperator::kLeftAntiJoin:
        case RelationalOperator::kLeftOuterJoin:
        case RelationalOperator::kLeftSemiJoin: {
          const HashJoinOperator &hash_join_op =
              static_cast<const HashJoinOperator&>(node);
          input_relation = &hash_join_op.probe_relation();
          input_relation_info = "probe_side";
          break;
        }
        case RelationalOperator::kNestedLoopsJoin: {
          const NestedLoopsJoinOperator &nlj_op =
              static_cast<const NestedLoopsJoinOperator&>(node);

          const CatalogRelation &left_input_relation = nlj_op.left_input_relation();
          const CatalogRelation &right_input_relation = nlj_op.right_input_relation();

          if (!left_input_relation.isTemporary()) {
            txt << "{\"input_relation\": \""
                << left_input_relation.getName()
                << "\", \"input_relation_id\": "
                << left_input_relation.getID()
                << ", \"type\": "
                << "\"left\""
                << ", \"proto\":\n\""
                << replaceQuote(left_input_relation.getProto().ShortDebugString())
                << "\"}";
            if (!right_input_relation.isTemporary()) {
              txt << ", ";
            }
          }

          if (!right_input_relation.isTemporary()) {
            txt << "{\"input_relation\": \""
                << right_input_relation.getName()
                << "\", \"input_relation_id\": "
                << right_input_relation.getID()
                << ", \"type\": "
                << "\"right\""
                << ", \"proto\":\n\""
                << replaceQuote(right_input_relation.getProto().ShortDebugString())
                << "\"}";
          }
          break;
        }
        case RelationalOperator::kSelect: {
          const SelectOperator &select_op =
              static_cast<const SelectOperator&>(node);
          input_relation = &select_op.input_relation();
          input_relation_info = "input";
          break;
        }
        case RelationalOperator::kSortRunGeneration: {
          const SortRunGenerationOperator &sort_op =
              static_cast<const SortRunGenerationOperator&>(node);
          input_relation = &sort_op.input_relation();
          input_relation_info = "input";
          break;
        }
        case RelationalOperator::kUnionAll: {
          const UnionAllOperator &union_all_op = static_cast<const UnionAllOperator&>(node);

          std::string input_stored_relation_names;
          std::size_t num_input_stored_relations = 0;
          bool first = true;
          for (const auto &input_relation : union_all_op.input_relations()) {
            if (input_relation->isTemporary()) {
              continue;
            }

            if (first) {
              first = false;
            } else {
              txt << ", ";
            }

            txt << "{input_relation\": \""
                << input_relation->getName()
                << "\", \"input_relation_id\": "
                << input_relation->getID()
                << ", \"type\": \""
                << num_input_stored_relations
                << "\", \"proto\":\n\""
                << replaceQuote(input_relation->getProto().ShortDebugString())
                << "\"}";

            ++num_input_stored_relations;
          }
          break;
        }
        default:
          break;
      }

      if (input_relation && !input_relation->isTemporary()) {
        txt << "{\"input_relation\": \""
            << input_relation->getName()
            << "\", \"input_relation_id\": "
            << input_relation->getID()
            << ", \"type\": \""
            << input_relation_info
            << "\", \"proto\":\n\""
            << replaceQuote(input_relation->getProto().ShortDebugString())
            << "\"}";
      }
      txt << "]}\n";
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
        << ", \"query context proto\": \n\""
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
