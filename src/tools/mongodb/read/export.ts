import z from "zod";
import { ObjectId } from "bson";
import type { AggregationCursor, FindCursor } from "mongodb";
import type { Document } from "bson";
import type { CallToolResult } from "@modelcontextprotocol/sdk/types.js";
import type { OperationType, ToolArgs, ToolExecutionContext } from "../../tool.js";
import { DbOperationArgs, MongoDBToolBase } from "../mongodbTool.js";
import { FindArgs } from "./find.js";
import { jsonExportFormat } from "../../../common/exportsManager.js";
import { getAggregateArgs } from "./aggregate.js";

const HumanizeLookupSchema = z.object({
    localField: z.string().min(1).describe("Local field path whose id value should be resolved."),
    fromCollection: z.string().min(1).describe("Collection to lookup human-readable values from."),
    foreignField: z.string().min(1).describe("Field in the foreign collection to match with localField."),
    asField: z.string().min(1).describe("Output field where lookup results will be stored."),
    select: z.array(z.string().min(1)).optional().describe("Optional list of fields to keep from lookup results."),
});

const HumanizeConfigSchema = z.object({
    lookups: z.array(HumanizeLookupSchema).min(1).describe("Lookup definitions for replacing ids with richer data."),
    dropRawIdFields: z
        .boolean()
        .optional()
        .describe("When true, removes localField id fields after the lookups are applied."),
});

type HumanizeLookup = z.infer<typeof HumanizeLookupSchema>;
type HumanizeConfig = z.infer<typeof HumanizeConfigSchema>;

const humanizePresets = {
    software_review_humanized: {
        lookups: [
            {
                localField: "software_id",
                fromCollection: "softwares",
                foreignField: "_id",
                asField: "software",
                select: ["software_name", "slug"],
            },
            {
                localField: "features.category",
                fromCollection: "software-category",
                foreignField: "_id",
                asField: "category_details",
                select: ["name"],
            },
        ],
        dropRawIdFields: true,
    } satisfies HumanizeConfig,
} as const satisfies Record<string, HumanizeConfig>;

export class ExportTool extends MongoDBToolBase {
    static toolName = "export";
    public description = "Export a query or aggregation results in the specified EJSON format.";
    public argsShape = {
        ...DbOperationArgs,
        exportTitle: z.string().describe("A short description to uniquely identify the export."),
        // Note: Although it is not required to wrap the discriminated union in
        // an array here because we only expect exactly one exportTarget to be
        // provided here, we unfortunately cannot use the discriminatedUnion as
        // is because Cursor is unable to construct payload for tool calls where
        // the input schema contains a discriminated union without such
        // wrapping. This is a workaround for enabling the tool calls on Cursor.
        exportTarget: z
            .array(
                z.discriminatedUnion("name", [
                    z.object({
                        name: z
                            .literal("find")
                            .describe("The literal name 'find' to represent a find cursor as target."),
                        arguments: z
                            .object({
                                ...FindArgs,
                                limit: FindArgs.limit.removeDefault(),
                            })
                            .describe("The arguments for 'find' operation."),
                    }),
                    z.object({
                        name: z
                            .literal("aggregate")
                            .describe("The literal name 'aggregate' to represent an aggregation cursor as target."),
                        arguments: z
                            .object(getAggregateArgs(this.isFeatureEnabled("search")))
                            .describe("The arguments for 'aggregate' operation."),
                    }),
                ])
            )
            .describe("The export target along with its arguments."),
        jsonExportFormat: jsonExportFormat
            .default("relaxed")
            .describe(
                [
                    "The format to be used when exporting collection data as EJSON with default being relaxed.",
                    "relaxed: A string format that emphasizes readability and interoperability at the expense of type preservation. That is, conversion from relaxed format to BSON can lose type information.",
                    "canonical: A string format that emphasizes type preservation at the expense of readability and interoperability. That is, conversion from canonical to BSON will generally preserve type information except in certain specific cases.",
                ].join("\n")
            ),
        outputPath: z
            .string()
            .optional()
            .describe(
                "Absolute or relative output file path where the exported JSON should be written. If omitted, server-managed exportsPath is used."
            ),
        humanize: HumanizeConfigSchema.optional().describe(
            "Optional ID-to-details mapping configuration. Lookups are appended as aggregation stages before writing the export."
        ),
        preset: z
            .string()
            .optional()
            .describe(
                `Optional reusable humanization preset name. Currently supported: ${Object.keys(humanizePresets).join(", ")}`
            ),
    };
    static operationType: OperationType = "read";

    protected async execute(
        {
            database,
            collection,
            jsonExportFormat,
            exportTitle,
            exportTarget: target,
            outputPath,
            humanize,
            preset,
        }: ToolArgs<typeof this.argsShape>,
        { signal }: ToolExecutionContext
    ): Promise<CallToolResult> {
        const provider = await this.ensureConnected();
        const exportTarget = target[0];
        if (!exportTarget) {
            throw new Error("Export target not provided. Expected one of the following: `aggregate`, `find`");
        }

        const resolvedHumanizeConfig = this.resolveHumanizeConfig({ preset, humanize });
        const humanizeStages = this.toHumanizeStages(resolvedHumanizeConfig);

        let cursor: FindCursor | AggregationCursor;
        if (exportTarget.name === "find") {
            const { filter, projection, sort, limit } = exportTarget.arguments;
            if (humanizeStages.length > 0) {
                const pipeline: Document[] = [{ $match: filter ?? {} }];
                if (sort) {
                    pipeline.push({ $sort: sort });
                }
                if (typeof limit === "number") {
                    pipeline.push({ $limit: limit });
                }
                pipeline.push(...humanizeStages);
                if (projection) {
                    pipeline.push({ $project: projection });
                }
                cursor = provider.aggregate(database, collection, pipeline, {
                    promoteValues: false,
                    bsonRegExp: true,
                    allowDiskUse: true,
                    signal,
                });
            } else {
                cursor = provider.find(database, collection, filter ?? {}, {
                    projection,
                    sort,
                    limit,
                    promoteValues: false,
                    bsonRegExp: true,
                    signal,
                });
            }
        } else {
            const { pipeline } = exportTarget.arguments;
            cursor = provider.aggregate(database, collection, [...pipeline, ...humanizeStages], {
                promoteValues: false,
                bsonRegExp: true,
                allowDiskUse: true,
                signal,
            });
        }

        const exportName = `${new ObjectId().toString()}.json`;

        const { exportURI, exportPath } = await this.session.exportsManager.createJSONExport({
            input: cursor,
            exportName,
            exportTitle:
                exportTitle ||
                `Export for namespace ${database}.${collection} requested on ${new Date().toLocaleString()}`,
            jsonExportFormat,
            outputPath,
        });
        const toolCallContent: CallToolResult["content"] = [
            // Not all the clients as of this commit understands how to
            // parse a resource_link so we provide a text result for them to
            // understand what to do with the result.
            {
                type: "text",
                text: `Data for namespace ${database}.${collection} is being exported and will be made available under resource URI - "${exportURI}".`,
            },
            {
                type: "resource_link",
                name: exportName,
                uri: exportURI,
                description: "Resource URI for fetching exported data once it is ready.",
                mimeType: "application/json",
            },
        ];

        // This special case is to make it easier to work with exported data for
        // clients that still cannot reference resources (Cursor).
        // More information here: https://jira.mongodb.org/browse/MCP-104
        if (this.isServerRunningLocally()) {
            toolCallContent.push({
                type: "text",
                text: `Optionally, when the export is finished, the exported data can also be accessed under path - "${exportPath}"`,
            });
        }

        return {
            content: toolCallContent,
        };
    }

    private isServerRunningLocally(): boolean {
        return this.config.transport === "stdio" || ["127.0.0.1", "localhost"].includes(this.config.httpHost);
    }

    private resolveHumanizeConfig({
        preset,
        humanize,
    }: {
        preset: string | undefined;
        humanize: HumanizeConfig | undefined;
    }): HumanizeConfig | undefined {
        const presetConfig = preset ? humanizePresets[preset as keyof typeof humanizePresets] : undefined;
        if (preset && !presetConfig) {
            throw new Error(
                `Unknown export preset "${preset}". Supported presets: ${Object.keys(humanizePresets).join(", ")}`
            );
        }

        if (!presetConfig) {
            return humanize;
        }

        if (!humanize) {
            return presetConfig;
        }

        return {
            lookups: [...presetConfig.lookups, ...humanize.lookups],
            dropRawIdFields: humanize.dropRawIdFields ?? presetConfig.dropRawIdFields,
        };
    }

    private toHumanizeStages(humanize: HumanizeConfig | undefined): Document[] {
        if (!humanize) {
            return [];
        }

        const stages: Document[] = [];
        for (const lookup of humanize.lookups) {
            stages.push(this.toLookupStage(lookup));
            if (lookup.select && lookup.select.length > 0) {
                stages.push(this.toLookupSelectionStage(lookup));
            }
        }

        if (humanize.dropRawIdFields) {
            const fields = [...new Set(humanize.lookups.map(({ localField }) => localField))];
            if (fields.length > 0) {
                stages.push({ $unset: fields });
            }
        }
        return stages;
    }

    private toLookupStage(lookup: HumanizeLookup): Document {
        return {
            $lookup: {
                from: lookup.fromCollection,
                localField: lookup.localField,
                foreignField: lookup.foreignField,
                as: lookup.asField,
            },
        };
    }

    private toLookupSelectionStage(lookup: HumanizeLookup): Document {
        const projection = Object.fromEntries(
            (lookup.select ?? []).map((fieldName) => [fieldName, `$$lookupDoc.${fieldName}`])
        );

        return {
            $addFields: {
                [lookup.asField]: {
                    $map: {
                        input: `$${lookup.asField}`,
                        as: "lookupDoc",
                        in: projection,
                    },
                },
            },
        };
    }
}
