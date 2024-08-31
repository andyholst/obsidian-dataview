import { FullIndex } from "data-index/index";
import { Context, LinkHandler } from "expression/context";
import { resolveSource, Datarow, matchingSourcePaths } from "data-index/resolver";
import { DataObject, Link, Literal, Values, Grouping, Widgets } from "data-model/value";
import { CalendarQuery, ListQuery, Query, QueryOperation, TableQuery } from "query/query";
import { Result } from "api/result";
import { Field, Fields } from "expression/field";
import { QuerySettings } from "settings";
import { DateTime } from "luxon";
import { SListItem } from "data-model/serialized/markdown";

function iden<T>(x: T): T {
    return x;
}

/** Interface for capturing diagnostic information during query execution. */
export interface OperationDiagnostics {
    timeMs: number;
    incomingRows: number;
    outgoingRows: number;
    errors: ExecutionError[];
}

/** Represents the meaning of an identifier within the context of a query execution. */
export type IdentifierMeaning = { type: "group"; name: string; on: IdentifierMeaning } | { type: "path" };

/** Type alias for a row of data associated with an object. */
export type Pagerow = Datarow<DataObject>;

/** Represents an error encountered during query execution. */
export type ExecutionError = { index: number; message: string };

/** Interface for the core execution results, including timing, data, and operations. */
export interface CoreExecution {
    data: Pagerow[];
    idMeaning: IdentifierMeaning;
    timeMs: number;
    ops: QueryOperation[];
    diagnostics: OperationDiagnostics[];
}

/**
 * Executes a series of query operations on a set of data rows.
 * @param rows The data rows to operate on.
 * @param context The execution context for evaluating operations.
 * @param ops The list of operations to execute.
 * @returns A result containing the execution details or an error message.
 */
function executeCore(rows: Pagerow[], context: Context, ops: QueryOperation[]): Result<CoreExecution, string> {
    const diagnostics: OperationDiagnostics[] = [];
    let identMeaning: IdentifierMeaning = { type: "path" };
    const startTime = Date.now();

    for (const op of ops) {
        const opStartTime = Date.now();
        const incomingRows = rows.length;
        const errors: ExecutionError[] = [];

        switch (op.type) {
            case "where": {
                rows = rows.filter((row, index) => {
                    const valueResult = context.evaluate(op.clause, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index, message: valueResult.error });
                        return false;
                    }
                    return Values.isTruthy(valueResult.value);
                });
                break;
            }
            case "sort": {
                rows.sort((a, b) => {
                    for (const { field, direction } of op.fields) {
                        const factor = direction === "ascending" ? 1 : -1;
                        const valAResult = context.evaluate(field, a.data);
                        const valBResult = context.evaluate(field, b.data);

                        if (valAResult.successful && valBResult.successful) {
                            const valA = valAResult.value;
                            const valB = valBResult.value;
                            if (context.binaryOps.evaluate("<", valA, valB, context).orElse(false)) return factor * -1;
                            if (context.binaryOps.evaluate(">", valA, valB, context).orElse(false)) return factor * 1;
                        } else {
                            if (!valAResult.successful) {
                                errors.push({ index: rows.indexOf(a), message: valAResult.error });
                            }
                            if (!valBResult.successful) {
                                errors.push({ index: rows.indexOf(b), message: valBResult.error });
                            }
                        }
                    }
                    return 0;
                });
                break;
            }
            case "limit": {
                const limitResult = context.evaluate(op.amount);
                if (!limitResult.successful) {
                    return Result.failure(`Failed to execute 'limit' statement: ${limitResult.error}`);
                }

                if (!Values.isNumber(limitResult.value)) {
                    return Result.failure(`Failed to execute 'limit' statement: expected a number, got ${typeof limitResult.value}`);
                }

                rows = rows.slice(0, limitResult.value);
                break;
            }
            case "group": {
                const groupData = rows.reduce<{ data: Pagerow; key: Literal }[]>((acc, row, index) => {
                    const valueResult = context.evaluate(op.field.field, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index, message: valueResult.error });
                        return acc;
                    }
                    acc.push({ data: row, key: valueResult.value });
                    return acc;
                }, []);

                groupData.sort((a, b) => {
                    if (context.binaryOps.evaluate("<", a.key, b.key, context).orElse(false)) return -1;
                    if (context.binaryOps.evaluate(">", a.key, b.key, context).orElse(false)) return 1;
                    return 0;
                });

                const finalGroupData = groupData.reduce<{ key: Literal; rows: DataObject[]; [groupKey: string]: Literal }[]>((acc, { key, data }) => {
                    const lastGroup = acc[acc.length - 1];
                    if (!lastGroup || !context.binaryOps.evaluate("=", key, lastGroup.key, context).orElse(false)) {
                        acc.push({ key, rows: [data.data], [op.field.name]: key });
                    } else {
                        lastGroup.rows.push(data.data);
                    }
                    return acc;
                }, []);

                rows = finalGroupData.map(d => ({ id: d.key, data: d }));
                identMeaning = { type: "group", name: op.field.name, on: identMeaning };
                break;
            }
            case "flatten": {
                const flattenResult = rows.reduce<Pagerow[]>((acc, row) => {
                    const valueResult = context.evaluate(op.field.field, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index: rows.indexOf(row), message: valueResult.error });
                        return acc;
                    }
                    const datapoints = Values.isArray(valueResult.value) ? valueResult.value : [valueResult.value];
                    datapoints.forEach(v => {
                        const copy = { ...row, data: { ...row.data, [op.field.name]: v } };
                        acc.push(copy);
                    });
                    return acc;
                }, []);

                rows = flattenResult;
                if (identMeaning.type == "group" && identMeaning.name == op.field.name) {
                    identMeaning = identMeaning.on;
                }
                break;
            }
            default:
                return Result.failure(`Unrecognized query operation '${op.type}'`);
        }

        if (errors.length >= incomingRows && incomingRows > 0) {
            return Result.failure(`Every row during operation '${op.type}' failed with an error; first ${Math.min(3, errors.length)}:\n${errors.slice(0, 3).map(d => "- " + d.message).join("\n")}`);
        }

        diagnostics.push({
            incomingRows,
            errors,
            outgoingRows: rows.length,
            timeMs: Date.now() - opStartTime,
        });
    }

    return Result.success({
        data: rows,
        idMeaning: identMeaning,
        ops,
        diagnostics,
        timeMs: Date.now() - startTime,
    });
}

/**
 * Executes a core query and then extracts specific fields from the result.
 * @param rows The data rows to operate on.
 * @param context The execution context for evaluating operations.
 * @param ops The list of operations to execute.
 * @param fields The fields to extract after the operations.
 * @returns A result containing the extracted data or an error message.
 */
export function executeCoreExtract(rows: Pagerow[], context: Context, ops: QueryOperation[], fields: Record<string, Field>): Result<CoreExecution, string> {
    const internal = executeCore(rows, context, ops);
    if (!internal.successful) return internal;

    const core = internal.value;
    const startTime = Date.now();
    const errors: ExecutionError[] = [];
    const res = core.data.reduce<Pagerow[]>((acc, row, index) => {
        const page: Pagerow = { id: row.id, data: {} };
        let isValid = true;

        for (const [name, field] of Object.entries(fields)) {
            const valueResult = context.evaluate(field, row.data);
            if (!valueResult.successful) {
                errors.push({ index, message: valueResult.error });
                isValid = false;
                break;
            }
            page.data[name] = valueResult.value;
        }

        if (isValid) acc.push(page);
        return acc;
    }, []);

    if (errors.length >= core.data.length && core.data.length > 0) {
        return Result.failure(`Every row during final data extraction failed with an error; first ${Math.max(errors.length, 3)}:\n${errors.slice(0, 3).map(d => "- " + d.message).join("\n")}`);
    }

    const execTime = Date.now() - startTime;
    return Result.success({
        data: res,
        idMeaning: core.idMeaning,
        diagnostics: core.diagnostics.concat([{ timeMs: execTime, incomingRows: core.data.length, outgoingRows: res.length, errors }]),
        ops: core.ops.concat([{ type: "extract", fields }]),
        timeMs: core.timeMs + execTime,
    });
}

/** Interface for the result of executing a list-based query. */
export interface ListExecution {
    core: CoreExecution;
    data: Literal[];
    primaryMeaning: IdentifierMeaning;
}

/**
 * Executes a list-based query and returns the final results.
 * @param query The query to execute.
 * @param index The full index to resolve the query against.
 * @param origin The origin of the query.
 * @param settings The query settings.
 * @returns A result containing the list execution data or an error message.
 */
export async function executeList(query: Query, index: FullIndex, origin: string, settings: QuerySettings): Promise<Result<ListExecution, string>> {
    const fileset = await resolveSource(query.source, index, origin);
    if (!fileset.successful) return Result.failure(fileset.error);

    const rootContext = new Context(defaultLinkHandler(index, origin), settings, {
        this: index.pages.get(origin)?.serialize(index) ?? {},
    });

    const { format: targetField, showId } = query.header as ListQuery;
    const fields: Record<string, Field> = targetField ? { target: targetField } : {};

    return executeCoreExtract(fileset.value, rootContext, query.operations, fields).map(core => {
        const data = showId && targetField
            ? core.data.map(p => Widgets.listPair(p.id, p.data["target"] ?? null))
            : targetField
                ? core.data.map(p => p.data["target"] ?? null)
                : core.data.map(p => p.id);
        return { primaryMeaning: core.idMeaning, core, data };
    });
}

/** Interface for the result of executing a table query. */
export interface TableExecution {
    core: CoreExecution;
    names: string[];
    data: Literal[][];
    idMeaning: IdentifierMeaning;
}

/**
 * Executes a table-based query and returns the final results.
 * @param query The query to execute.
 * @param index The full index to resolve the query against.
 * @param origin The origin of the query.
 * @param settings The query settings.
 * @returns A result containing the table execution data or an error message.
 */
export async function executeTable(query: Query, index: FullIndex, origin: string, settings: QuerySettings): Promise<Result<TableExecution, string>> {
    const fileset = await resolveSource(query.source, index, origin);
    if (!fileset.successful) return Result.failure(fileset.error);

    const rootContext = new Context(defaultLinkHandler(index, origin), settings, {
        this: index.pages.get(origin)?.serialize(index) ?? {},
    });

    const { fields: targetFields, showId } = query.header as TableQuery;
    const fields: Record<string, Field> = {};
    for (const field of targetFields) fields[field.name] = field.field;

    return executeCoreExtract(fileset.value, rootContext, query.operations, fields).map(core => {
        const names = showId
            ? [core.idMeaning.type === "group" ? core.idMeaning.name : settings.tableIdColumnName, ...targetFields.map(f => f.name)]
            : targetFields.map(f => f.name);

        const data = showId
            ? core.data.map(p => ([p.id] as Literal[]).concat(targetFields.map(f => p.data[f.name])))
            : core.data.map(p => targetFields.map(f => p.data[f.name]));

        return { core, names, data, idMeaning: core.idMeaning };
    });
}

/** Interface for the result of executing a task-based query. */
export interface TaskExecution {
    core: CoreExecution;
    tasks: Grouping<SListItem>;
}

/**
 * Recursively extracts task groupings from the result of a query execution.
 * @param id The identifier meaning for grouping.
 * @param rows The data rows to extract from.
 * @returns The extracted task groupings.
 */
function extractTaskGroupings(id: IdentifierMeaning, rows: DataObject[]): Grouping<SListItem> {
    return id.type === "path"
        ? rows as SListItem[]
        : rows.map(r => iden({
            key: r[id.name],
            rows: extractTaskGroupings(id.on, r.rows as DataObject[]),
        }));
}

/**
 * Executes a task-based query and returns all matching tasks.
 * @param query The query to execute.
 * @param origin The origin of the query.
 * @param index The full index to resolve the query against.
 * @param settings The query settings.
 * @returns A result containing the task execution data or an error message.
 */
export async function executeTask(query: Query, origin: string, index: FullIndex, settings: QuerySettings): Promise<Result<TaskExecution, string>> {
    const fileset = matchingSourcePaths(query.source, index, origin);
    if (!fileset.successful) return Result.failure(fileset.error);

    const incomingTasks: Pagerow[] = [];
    for (const path of fileset.value) {
        const page = index.pages.get(path);
        if (!page) continue;

        const pageData = page.serialize(index);
        const pageTasks = pageData.file.tasks.map(t => {
            const tcopy = Values.deepCopy(t);
            for (const [key, value] of Object.entries(pageData)) {
                if (!(key in tcopy)) {
                    tcopy[key] = value;
                }
            }
            return { id: `${pageData.path}#${t.line}`, data: tcopy };
        });

        incomingTasks.push(...pageTasks);
    }

    const rootContext = new Context(defaultLinkHandler(index, origin), settings, {
        this: index.pages.get(origin)?.serialize(index) ?? {},
    });

    return executeCore(incomingTasks, rootContext, query.operations).map(core => ({
        core,
        tasks: extractTaskGroupings(core.idMeaning, core.data.map(r => r.data)),
    }));
}

/**
 * Executes an inline query for a single field and returns the evaluated result.
 * @param field The field to evaluate.
 * @param origin The origin of the query.
 * @param index The full index to resolve the query against.
 * @param settings The query settings.
 * @returns A result containing the evaluated value or an error message.
 */
export function executeInline(field: Field, origin: string, index: FullIndex, settings: QuerySettings): Result<Literal, string> {
    const context = new Context(defaultLinkHandler(index, origin), settings, {
        this: index.pages.get(origin)?.serialize(index) ?? {},
    });

    const result = context.evaluate(field);
    if (!result.successful) {
        return Result.failure(result.error);
    }

    return Result.success(result.value);
}

/**
 * Creates a default link handler for resolving links within a query context.
 * @param index The full index to resolve links against.
 * @param origin The origin of the query.
 * @returns A link handler for resolving, normalizing, and checking link existence.
 */
export function defaultLinkHandler(index: FullIndex, origin: string): LinkHandler {
    return {
        resolve: link => {
            const realFile = index.metadataCache.getFirstLinkpathDest(link, origin);
            if (!realFile) return null;

            const realPage = index.pages.get(realFile.path);
            return realPage ? realPage.serialize(index) : null;
        },
        normalize: link => {
            const realFile = index.metadataCache.getFirstLinkpathDest(link, origin);
            return realFile?.path ?? link;
        },
        exists: link => !!index.metadataCache.getFirstLinkpathDest(link, origin),
    };
}

/** Interface for the result of executing a calendar-based query. */
export interface CalendarExecution {
    core: CoreExecution;
    data: { date: DateTime; link: Link; value?: Literal[] }[];
}

/**
 * Executes a calendar-based query and returns the final results.
 * @param query The query to execute.
 * @param index The full index to resolve the query against.
 * @param origin The origin of the query.
 * @param settings The query settings.
 * @returns A result containing the calendar execution data or an error message.
 */
export async function executeCalendar(query: Query, index: FullIndex, origin: string, settings: QuerySettings): Promise<Result<CalendarExecution, string>> {
    const fileset = await resolveSource(query.source, index, origin);
    if (!fileset.successful) return Result.failure(fileset.error);

    const rootContext = new Context(defaultLinkHandler(index, origin), settings, {
        this: index.pages.get(origin)?.serialize(index) ?? {},
    });

    const targetField = (query.header as CalendarQuery).field.field;
    const fields: Record<string, Field> = {
        target: targetField,
        link: Fields.indexVariable("file.link"),
    };

    return executeCoreExtract(fileset.value, rootContext, query.operations, fields).map(core => {
        const data = core.data.map(p => iden({
            date: p.data["target"] as DateTime,
            link: p.data["link"] as Link,
        }));
        return { core, data };
    });
}
