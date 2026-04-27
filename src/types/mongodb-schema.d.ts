declare module "mongodb-schema" {
    import type { AggregationCursor, Document, FindCursor } from "mongodb";
    import type { Readable } from "stream";

    export type SimplifiedSchema = Record<string, unknown>;

    export function getSimplifiedSchema(
        source: Document[] | AggregationCursor | FindCursor | Readable
    ): Promise<SimplifiedSchema>;
}
