import React, {useMemo, useState} from "react";
import type {ChainOfEventsGraph} from "../model/ChainOfEventsGraph.ts";
import { ForceGraph2D } from "react-force-graph";

type ChainOfEventsGraphProps = {
    chainOfEvents: ChainOfEventsGraph;
}

const ChainOfEventsGraph: React.FC<ChainOfEventsGraphProps> = ({ chainOfEvents }) => {
    const [hoveredNodeId, setHoveredNodeId] = useState<string | null>(null);
    const [tooltipPosition, setToolTipPosition] = useState<{ x: number, y: number } | null>(null);

    const mapChainOfEventsToGraph = (chainOfEvents: ChainOfEventsGraph)  => {
        const { graph } = chainOfEvents;
        const nodes = Object.values(graph);
        const edges = nodes.flatMap(
            node => node.predecessors.map(predecessor => ({ source: predecessor, target: node.id }))
        );
        return {
            nodes: nodes.map(node => ({
                id: node.id,
                label: node.log ? node.log.message : node.span ? node.span.clusterEvent : "",
                name: node.log ? node.log.message : node.span ? node.span.clusterEvent : "",
                color: node.id === chainOfEvents.rootId ? "red" : "blue"
            })),
            links: edges
        }
    }

    const graphData = useMemo(() => mapChainOfEventsToGraph(chainOfEvents), [chainOfEvents]);

    const getDetailsFromId = (id: string) => {
        const node = chainOfEvents.graph[id];
        return node.log ? JSON.stringify(node.log, null, 2) : node.span ? JSON.stringify(node.span, null, 2) : "";
    }

    return (
       <div>
            <ForceGraph2D
                graphData={graphData}
                nodeAutoColorBy="color"
                onNodeHover={(node) => {
                    console.log(node);
                    setHoveredNodeId(node ? node.id : null);
                    setToolTipPosition(node && node.x && node.y ? { x: node.x, y: node.y } : null);
                }}
                linkDirectionalArrowLength={6} // Length of the arrowhead
                linkDirectionalArrowRelPos={1} // Position of the arrowhead (1 = end of the link)
                linkDirectionalArrowColor={() => 'blue'} // Arrow color
            />
           {hoveredNodeId && tooltipPosition && (
               <div
                   style={{
                       all: 'unset',
                       position: 'absolute',
                       top: tooltipPosition.y, // Offset to avoid overlapping mouse
                       left: tooltipPosition.x,
                       backgroundColor: 'rgba(0, 0, 0, 0.8)',
                       color: 'white',
                       padding: '5px 10px',
                       borderRadius: '5px',
                       pointerEvents: 'none',
                       zIndex: 1000,
                   }}
               >
                   <div><strong>ID:</strong> {hoveredNodeId}</div>
                   {<div><strong>Details:</strong> {getDetailsFromId(hoveredNodeId)}</div>}
               </div>
           )}
       </div>
    );
}

export default ChainOfEventsGraph;