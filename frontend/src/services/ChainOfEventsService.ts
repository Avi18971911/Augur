import {HandlerChainOfEventsNodeDTO, HandlerChainOfEventsResponseDTO} from "../backend_api";
import {ChainOfEventsGraph, ChainOfEventsGraphNode} from "../model/ChainOfEventsGraph.ts";
import {mapLogDtoToLog, mapSpanDTOToSpan} from "./LogOrSpanService.ts";

export const mapChainOfEventsResponseToChainOfEventsGraph = (
    response: HandlerChainOfEventsResponseDTO
): ChainOfEventsGraph => {
    const ret: Record<string, ChainOfEventsGraphNode> = {};
    Object.entries(response.graph).forEach(([key, value]) => {
        ret[key] = mapChainOfEventsNodeDTOToChainOfEventsGraphNode(value)
    })
    return ret;
}

const mapChainOfEventsNodeDTOToChainOfEventsGraphNode = (
    node: HandlerChainOfEventsNodeDTO
): ChainOfEventsGraphNode  => {
    return {
        id: node.id,
        clusterId: node.clusterId,
        log: node.logDto ? mapLogDtoToLog(node.logDto) : undefined,
        span: node.spanDto ? mapSpanDTOToSpan(node.spanDto) : undefined,
        predecessors: node.predecessors,
        successors: node.successors
    }
}
