import {HandlerChainOfEventsNodeDTO, HandlerChainOfEventsResponseDTO, HandlerEdgeDTO} from "../backend_api";
import {ChainOfEventsGraph, ChainOfEventsGraphNode} from "../model/ChainOfEventsGraph.ts";
import {mapLogDtoToLog, mapSpanDTOToSpan} from "./LogOrSpanService.ts";

export const mapChainOfEventsResponseToChainOfEventsGraph = (
    response: HandlerChainOfEventsResponseDTO,
    rootClusterId: string,
): ChainOfEventsGraph => {
    const ret: Record<string, ChainOfEventsGraphNode> = {};
    Object.entries(response.graph).forEach(([key, value]) => {
        ret[key] = mapChainOfEventsNodeDTOToChainOfEventsGraphNode(value)
    })
    return {
        graph: ret,
        rootClusterId: rootClusterId
    };
}

const mapChainOfEventsNodeDTOToChainOfEventsGraphNode = (
    node: HandlerChainOfEventsNodeDTO
): ChainOfEventsGraphNode  => {
    const smallestPredecessor = findSmallestEdge(node.predecessors, false);
    const smallestSuccessor = findSmallestEdge(node.successors, true);
    return {
        clusterId: node.clusterId,
        log: node.logDto ? mapLogDtoToLog(node.logDto) : undefined,
        span: node.spanDto ? mapSpanDTOToSpan(node.spanDto) : undefined,
        predecessors: smallestPredecessor ? [smallestPredecessor] : [],
        successors: smallestSuccessor ? [smallestSuccessor] : []
    }
}

const findSmallestEdge = (edges: HandlerEdgeDTO[], successor: boolean) => {
    if (successor) {
        return edges.length > 0 ? edges.reduce(
            (prev, curr) => ((prev?.tdoa) && prev.tdoa < (curr.tdoa ?? 0))  ? prev : curr
        ).clusterId : undefined;
    } else {
        return edges.length > 0 ? edges.reduce(
            (prev, curr) => ((prev?.tdoa) && prev.tdoa > (curr.tdoa ?? 0)) ? prev : curr
        ).clusterId : undefined;
    }
}
