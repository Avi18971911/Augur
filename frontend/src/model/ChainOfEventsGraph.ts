import {Span} from "./Span.ts";
import {Log} from "./Log.ts";

export type ChainOfEventsGraph = Record<string, ChainOfEventsGraphNode>;

export type ChainOfEventsGraphNode = {
    id: string;
    clusterId: string;
    log?: Log;
    span?: Span;
    predecessors: string[];
    successors: string[];
}