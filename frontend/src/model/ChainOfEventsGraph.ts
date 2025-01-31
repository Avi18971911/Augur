import {Span} from "./Span.ts";
import {Log} from "./Log.ts";

export type ChainOfEventsGraph = {
    graph: Record<string, ChainOfEventsGraphNode>;
    rootClusterId: string;
}

export type ChainOfEventsGraphNode = {
    clusterId: string;
    log?: Log;
    span?: Span;
    predecessors: string[];
    successors: string[];
}