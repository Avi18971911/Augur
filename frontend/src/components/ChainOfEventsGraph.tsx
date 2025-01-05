import React from "react";
import type {ChainOfEventsGraph} from "../model/ChainOfEventsGraph.ts";
import { ForceGraph2D } from "react-force-graph";

type ChainOfEventsGraphProps = {
    chainOfEvents: ChainOfEventsGraph;
}

const mapChainOfEventsToGraph = (chainOfEvents: ChainOfEventsGraph)  => {
    const { graph } = chainOfEvents;
    const nodes = Object.values(graph);
    const edges = nodes.flatMap(node => node.predecessors.map(predecessor => ({ source: predecessor, target: node.id })));
    return {
        nodes: nodes.map(node => ({
            id: node.id,
            label: node.log ? node.log.message : node.span ? node.span.clusterEvent : "",
            color: node.id === chainOfEvents.rootId ? "red" : "blue"
        })),
        links: edges
    }
}

const ChainOfEventsGraph: React.FC<ChainOfEventsGraphProps> = ({ chainOfEvents }) => {
   return (
       <div>
            <ForceGraph2D
                graphData={mapChainOfEventsToGraph(chainOfEvents)}
                nodeAutoColorBy="color"
                linkDirectionalArrowLength={6} // Length of the arrowhead
                linkDirectionalArrowRelPos={1} // Position of the arrowhead (1 = end of the link)
                linkDirectionalArrowColor={() => 'blue'} // Arrow color
            />
       </div>
    );
}

export default ChainOfEventsGraph;