import {HandlerChainOfEventsNodeDTO, HandlerChainOfEventsResponseDTO, HandlerEdgeDTO} from "../backend_api";
import {ChainOfEventsGraph, ChainOfEventsGraphNode} from "../model/ChainOfEventsGraph.ts";
import {mapLogDtoToLog, mapSpanDTOToSpan} from "./LogOrSpanService.ts";

export const mapChainOfEventsResponseToChainOfEventsGraph = (
    response: HandlerChainOfEventsResponseDTO,
    rootId: string,
): ChainOfEventsGraph => {
    const ret: Record<string, ChainOfEventsGraphNode> = {};
    Object.entries(response.graph).forEach(([key, value]) => {
        ret[key] = mapChainOfEventsNodeDTOToChainOfEventsGraphNode(value)
    })
    return {
        graph: ret,
        rootId: rootId
    };
}

const mapChainOfEventsNodeDTOToChainOfEventsGraphNode = (
    node: HandlerChainOfEventsNodeDTO
): ChainOfEventsGraphNode  => {
    const smallestPredecessor = findSmallestEdge(node.predecessors, false);
    const smallestSuccessor = findSmallestEdge(node.successors, true);
    return {
        id: node.id,
        clusterId: node.clusterId,
        log: node.logDto ? mapLogDtoToLog(node.logDto) : undefined,
        span: node.spanDto ? mapSpanDTOToSpan(node.spanDto) : undefined,
        predecessors: smallestPredecessor ? [smallestPredecessor] : [],
        successors: smallestSuccessor ? [smallestSuccessor] : []
    }
}

const findSmallestEdge = (edges: HandlerEdgeDTO[], successor: boolean) => {
    if (successor) {
        return edges.length > 0 ? edges.reduce((prev, curr) => prev.tdoa < curr.tdoa ? prev : curr).id : undefined;
    } else {
        return edges.length > 0 ? edges.reduce((prev, curr) => prev.tdoa > curr.tdoa ? prev : curr).id : undefined;
    }
}
