import React, {useMemo} from "react";
import type {ChainOfEventsGraph} from "../model/ChainOfEventsGraph.ts";
import ForceGraph2D from "react-force-graph-2d";
import {Log} from "../model/Log.ts";
import {Span} from "../model/Span.ts";

type ChainOfEventsGraphProps = {
    chainOfEvents: ChainOfEventsGraph;
}

type LogNodeLabel = {
    message: string;
    timestamp: Date;
    severity: string;
    service: string;
}

type SpanNodeLabel = {
    service: string;
    startTime: Date;
    endTime: Date;
    actionName: string;
    spanKind: string;
}

const mapLogToNodeLabel = (log: Log): LogNodeLabel => {
    return {
        message: log.message,
        timestamp: log.timestamp,
        severity: log.severity,
        service: log.service
    }
}

const mapSpanToNodeLabel = (span: Span): SpanNodeLabel => {
    return {
        service: span.service,
        startTime: span.startTime,
        endTime: span.endTime,
        actionName: span.actionName,
        spanKind: span.spanKind
    }
}

const ChainOfEventsGraph: React.FC<ChainOfEventsGraphProps> = ({ chainOfEvents }) => {
    const getDetailsFromId = (id: string) => {
        const node = chainOfEvents.graph[id];
        return node.log ?
            JSON.stringify(mapLogToNodeLabel(node.log), null, 2)
            : node.span ?
                JSON.stringify(mapSpanToNodeLabel(node.span), null, 2)
                : "";
    }

    const mapChainOfEventsToGraph = (chainOfEvents: ChainOfEventsGraph)  => {
        const { graph } = chainOfEvents;
        const nodes = Object.values(graph);
        const edges = nodes.flatMap(
            node => node.predecessors.map(predecessor => ({ source: predecessor, target: node.id }))
        );
        return {
            nodes: nodes.map(node => ({
                id: node.id,
                label: getDetailsFromId(node.id),
                name: getDetailsFromId(node.id),
                color: node.id === chainOfEvents.rootId ? "red" : "blue"
            })),
            links: edges
        }
    }

    const graphData = useMemo(() => mapChainOfEventsToGraph(chainOfEvents), [chainOfEvents]);

    return (
       <div>
            <ForceGraph2D
                graphData={graphData}
                nodeAutoColorBy="color"
                linkDirectionalArrowLength={6} // Length of the arrowhead
                linkDirectionalArrowRelPos={1} // Position of the arrowhead (1 = end of the link)
                linkDirectionalArrowColor={() => 'blue'} // Arrow color
            />
       </div>
    );
}

export default ChainOfEventsGraph;