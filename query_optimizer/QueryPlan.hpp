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

#ifndef QUICKSTEP_QUERY_OPTIMIZER_QUERY_PLAN_HPP_
#define QUICKSTEP_QUERY_OPTIMIZER_QUERY_PLAN_HPP_

#include <unordered_map>
#include <utility>

#include "relational_operators/RelationalOperator.hpp"
#include "utility/DAG.hpp"
#include "utility/Macros.hpp"
#include "relational_operators/AggregationOperator.hpp"
#include "relational_operators/BuildHashOperator.hpp"
#include "relational_operators/BuildLIPFilterOperator.hpp"
#include "relational_operators/HashJoinOperator.hpp"
#include "relational_operators/NestedLoopsJoinOperator.hpp"
#include "relational_operators/RelationalOperator.hpp"
#include "relational_operators/SelectOperator.hpp"
#include "relational_operators/SortRunGenerationOperator.hpp"
#include "relational_operators/UnionAllOperator.hpp"

namespace quickstep {

/** \addtogroup QueryOptimizer
 *  @{
 */

/**
 * @brief A plan to execute a query.
 **/
class QueryPlan {
 public:
  typedef DAG<RelationalOperator, bool>::size_type_nodes DAGNodeIndex;

  /**
   * @brief Constructor.
   */
  QueryPlan() {}

  /**
   * @brief Create a relational operator node in the plan, and set its node
   *        index.
   *
   * @param relational_operator The relational operator to be added to the plan.
   * @return The node index in the exeuciton plan.
   */
  inline DAGNodeIndex addRelationalOperator(RelationalOperator *relational_operator) {
    const DAGNodeIndex node_index = dag_operators_.createNode(relational_operator);
    relational_operator->setOperatorIndex(node_index);
    addInputRelation(relational_operator);
    return node_index;
  }

  inline void addInputRelation(RelationalOperator *relational_operator) {
    const CatalogRelationSchema *input_relation = nullptr;
    const RelationalOperator::OperatorType node_type = relational_operator->getOperatorType();
    std::string input_relation_info;
    std::string txt = "";
    switch (node_type) {
      case RelationalOperator::kAggregation: {
        const AggregationOperator &aggregation_op =
            static_cast<const AggregationOperator&>(*relational_operator);
        input_relation = &aggregation_op.input_relation();
        input_relation_info = "input";
        break;
      }
      case RelationalOperator::kBuildHash: {
        const BuildHashOperator &build_hash_op =
            static_cast<const BuildHashOperator&>(*relational_operator);
        input_relation = &build_hash_op.input_relation();
        input_relation_info = "input";
        break;
      }
      case RelationalOperator::kBuildLIPFilter: {
        const BuildLIPFilterOperator &build_lip_filter_op =
            static_cast<const BuildLIPFilterOperator&>(*relational_operator);
        input_relation = &build_lip_filter_op.input_relation();
        input_relation_info = "input";
        break;
      }
      case RelationalOperator::kInnerJoin:
      case RelationalOperator::kLeftAntiJoin:
      case RelationalOperator::kLeftOuterJoin:
      case RelationalOperator::kLeftSemiJoin: {
        const HashJoinOperator &hash_join_op =
            static_cast<const HashJoinOperator&>(*relational_operator);
        input_relation = &hash_join_op.probe_relation();
        input_relation_info = "probe_side";
        break;
      }
      case RelationalOperator::kNestedLoopsJoin: {
        const NestedLoopsJoinOperator &nlj_op =
            static_cast<const NestedLoopsJoinOperator&>(*relational_operator);

        const CatalogRelation &left_input_relation = nlj_op.left_input_relation();
        const CatalogRelation &right_input_relation = nlj_op.right_input_relation();

        if (!left_input_relation.isTemporary()) {
          txt = "{\"input_relation\": \""
              + left_input_relation.getName()
              + "\", \"input_relation_id\": "
              + std::to_string(left_input_relation.getID())
              + ", \"type\": "
              + "\"left\""
              + ", \"proto\": \""
              + replaceQuote(left_input_relation.getProto().ShortDebugString())
              + "\"}";
          if (!right_input_relation.isTemporary()) {
            txt += ", ";
          }
        }

        if (!right_input_relation.isTemporary()) {
          txt += "{\"input_relation\": \""
              + right_input_relation.getName()
              + "\", \"input_relation_id\": "
              + std::to_string(right_input_relation.getID())
              + ", \"type\": "
              + "\"right\""
              + ", \"proto\": \""
              + replaceQuote(right_input_relation.getProto().ShortDebugString())
              + "\"}";
        }
        break;
      }
      case RelationalOperator::kSelect: {
        const SelectOperator &select_op =
            static_cast<const SelectOperator&>(*relational_operator);
        input_relation = &select_op.input_relation();
        input_relation_info = "input";
        break;
      }
      case RelationalOperator::kSortRunGeneration: {
        const SortRunGenerationOperator &sort_op =
            static_cast<const SortRunGenerationOperator&>(*relational_operator);
        input_relation = &sort_op.input_relation();
        input_relation_info = "input";
        break;
      }
      case RelationalOperator::kUnionAll: {
        const UnionAllOperator &union_all_op = static_cast<const UnionAllOperator&>(*relational_operator);

        std::string input_stored_relation_names;
        std::size_t num_input_stored_relations = 0;
        bool first = true;
        for (const auto &ir : union_all_op.input_relations()) {
          if (ir->isTemporary()) {
            continue;
          }

          if (first) {
            first = false;
          } else {
            txt += ", ";
          }

          txt += "{input_relation\": \""
              + ir->getName()
              + "\", \"input_relation_id\": "
              + std::to_string(ir->getID())
              + ", \"type\": \""
              + std::to_string(num_input_stored_relations)
              + "\", \"proto\": \""
              + replaceQuote(ir->getProto().ShortDebugString())
              + "\"}";

          ++num_input_stored_relations;
        }
        break;
      }
      default:
        break;
    }

    if (input_relation != nullptr && !input_relation->isTemporary()) {
      txt = "{\"input_relation\": \""
          + input_relation->getName()
          + "\", \"input_relation_id\": "
          + std::to_string(input_relation->getID())
          + ", \"type\": \""
          + input_relation_info
          + "\", \"proto\": \""
          + replaceQuote(input_relation->getProto().ShortDebugString())
          + "\"}";
    }
    input_relations_.push_back(txt);
  }

  /**
   * @brief Creates a link from \p producer_operator_index to \p consumer_operator_index
   *        in the DAG.
   *
   * @param consumer_operator_index The index of the consumer operator.
   * @param producer_operator_index The index of the producer operator.
   * @param is_pipeline_breaker True if the result from the producer cannot be
   *                            pipelined to the consumer, otherwise false.
   */
  inline void addDirectDependency(DAGNodeIndex consumer_operator_index,
                                  DAGNodeIndex producer_operator_index,
                                  bool is_pipeline_breaker) {
    dag_operators_.createLink(producer_operator_index, consumer_operator_index, is_pipeline_breaker);
  }

  /**
   * @brief Creates a link or upgrades the existing link from \p producer_operator_index
   *        to \p consumer_operator_index in the DAG.
   *
   * Depending on whether there is an existing link from \p producer_operator_index
   * to \p consumer_operator_index:
   *   - Case 1, no existing link:
   *         Creates a link with metadata set to is_pipeline_breaker.
   *   - Case 2, existing link with metadata \p m:
   *         Set m = (m | is_pipeline_break).
   *
   * @param consumer_operator_index The index of the consumer operator.
   * @param producer_operator_index The index of the producer operator.
   * @param is_pipeline_breaker True if the result from the producer cannot be
   *                            pipelined to the consumer, otherwise false.
   */
  inline void addOrUpgradeDirectDependency(DAGNodeIndex consumer_operator_index,
                                           DAGNodeIndex producer_operator_index,
                                           bool is_pipeline_breaker) {
    const auto &dependents = dag_operators_.getDependents(producer_operator_index);
    const auto consumer_it = dependents.find(consumer_operator_index);
    if (consumer_it == dependents.end()) {
      dag_operators_.createLink(producer_operator_index,
                                consumer_operator_index,
                                is_pipeline_breaker);
    } else {
      dag_operators_.setLinkMetadata(producer_operator_index,
                                     consumer_operator_index,
                                     consumer_it->second | is_pipeline_breaker);
    }
  }

  /**
   * @brief Creates dependencies for a DropTable operator with index
   *        \p drop_operator_index. If \p producer_operator_index
   *        has any dependent, creates a link from \p drop_operator_index
   *        to each dependent of \p producer_operator_index in the DAG;
   *        otherwise, create a direct link between \p drop_operator_index
   *        and \p producer_operator_index.
   *
   * @param dependent_operator_index The index of the DropTable operator node.
   * @param dependency_operator_index The index of the operator node that outputs
   *                                  the relation to be dropped by the operator
   *                                  at \p dependent_operator_index.
   */
  inline void addDependenciesForDropOperator(DAGNodeIndex drop_operator_index,
                                             DAGNodeIndex producer_operator_index) {
    const std::unordered_map<DAGNodeIndex, bool> &direct_dependents =
        dag_operators_.getDependents(producer_operator_index);
    if (!direct_dependents.empty()) {
      for (const std::pair<const DAGNodeIndex, bool> &direct_dependent : direct_dependents) {
        dag_operators_.createLink(direct_dependent.first,
                                  drop_operator_index,
                                  true /* is_pipeline_breaker */);
      }
    } else {
      dag_operators_.createLink(producer_operator_index,
                                drop_operator_index,
                                true /* is_pipeline_breaker */);
    }
  }

  /**
   * @return The DAG query plan.
   */
  const DAG<RelationalOperator, bool>& getQueryPlanDAG() const {
    return dag_operators_;
  }

  /**
   * @return The DAG query plan.
   */
  DAG<RelationalOperator, bool>* getQueryPlanDAGMutable() {
    return &dag_operators_;
  }

  const std::vector<std::string>& getInputRelations() const {
    return input_relations_;
  }

 private:
  // A false value of the second template argument indicates that the
  // link between two RelationalOperators is not a pipeline breaker.
  // while a true value indicates that the link is a pipeline
  // breaker. Streamed data won't move through pipeline breaker links.
  DAG<RelationalOperator, bool> dag_operators_;
  std::vector<std::string> input_relations_;

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

  DISALLOW_COPY_AND_ASSIGN(QueryPlan);
};

/** @} */

}  // namespace quickstep

#endif  // QUICKSTEP_QUERY_OPTIMIZER_QUERY_PLAN_HPP_
