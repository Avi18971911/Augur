import {Span} from "./Span.ts";
import {Log} from "./Log.ts";

export type ChainOfEventsGraph = {
    graph: Record<string, ChainOfEventsGraphNode>;
    rootId: string;
}

export type ChainOfEventsGraphNode = {
    id: string;
    clusterId: string;
    log?: Log;
    span?: Span;
    predecessors: string[];
    successors: string[];
}