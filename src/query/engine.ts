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

export interface OperationDiagnostics {
    timeMs: number;
    incomingRows: number;
    outgoingRows: number;
    errors: ExecutionError[];
}

export type IdentifierMeaning = { type: "group"; name: string; on: IdentifierMeaning } | { type: "path" };

export type Pagerow = Datarow<DataObject>;

export type ExecutionError = { index: number; message: string };

export interface CoreExecution {
    data: Pagerow[];
    idMeaning: IdentifierMeaning;
    timeMs: number;
    ops: QueryOperation[];
    diagnostics: OperationDiagnostics[];
}

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
                const whereResult = rows.filter((row, index) => {
                    const valueResult = context.evaluate(op.clause, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index, message: valueResult.error });
                        return false;
                    }
                    return Values.isTruthy(valueResult.value);
                });
                rows = whereResult;
                break;
            }
            case "sort": {
                const sortFields = op.fields;
                rows.sort((a, b) => {
                    for (const { field, direction } of sortFields) {
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
                const groupData: { data: Pagerow; key: Literal }[] = rows.map((row, index) => {
                    const valueResult = context.evaluate(op.field.field, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index, message: valueResult.error });
                        return null;
                    }
                    return { data: row, key: valueResult.value };
                }).filter(Boolean) as { data: Pagerow; key: Literal }[];

                groupData.sort((a, b) => {
                    if (context.binaryOps.evaluate("<", a.key, b.key, context).orElse(false)) return -1;
                    if (context.binaryOps.evaluate(">", a.key, b.key, context).orElse(false)) return 1;
                    return 0;
                });

                const finalGroupData: { key: Literal; rows: DataObject[]; [groupKey: string]: Literal }[] = [];
                for (const { key, data } of groupData) {
                    if (finalGroupData.length === 0 || !context.binaryOps.evaluate("=", key, finalGroupData[finalGroupData.length - 1].key, context).orElse(false)) {
                        finalGroupData.push({ key, rows: [data.data], [op.field.name]: key });
                    } else {
                        finalGroupData[finalGroupData.length - 1].rows.push(data.data);
                    }
                }
                rows = finalGroupData.map(d => ({ id: d.key, data: d }));
                identMeaning = { type: "group", name: op.field.name, on: identMeaning };
                break;
            }
            case "flatten": {
                const flattenResult: Pagerow[] = [];
                for (const row of rows) {
                    const valueResult = context.evaluate(op.field.field, row.data);
                    if (!valueResult.successful) {
                        errors.push({ index: rows.indexOf(row), message: valueResult.error });
                        continue;
                    }
                    const datapoints = Values.isArray(valueResult.value) ? valueResult.value : [valueResult.value];
                    for (const v of datapoints) {
                        const copy = Values.deepCopy(row);
                        copy.data[op.field.name] = v;
                        flattenResult.push(copy);
                    }
                }
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

export function executeCoreExtract(rows: Pagerow[], context: Context, ops: QueryOperation[], fields: Record<string, Field>): Result<CoreExecution, string> {
    const internal = executeCore(rows, context, ops);
    if (!internal.successful) return internal;

    const core = internal.value;
    const startTime = Date.now();
    const errors: ExecutionError[] = [];
    const res = core.data.map((row, index) => {
        const page: Pagerow = { id: row.id, data: {} };
        for (const [name, field] of Object.entries(fields)) {
            const valueResult = context.evaluate(field, row.data);
            if (!valueResult.successful) {
                errors.push({ index, message: valueResult.error });
                return null;
            }
            page.data[name] = valueResult.value;
        }
        return page;
    }).filter(Boolean) as Pagerow[];

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

export interface ListExecution {
    core: CoreExecution;
    data: Literal[];
    primaryMeaning: IdentifierMeaning;
}

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

export interface TableExecution {
    core: CoreExecution;
    names: string[];
    data: Literal[][];
    idMeaning: IdentifierMeaning;
}

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

export interface TaskExecution {
    core: CoreExecution;
    tasks: Grouping<SListItem>;
}

function extractTaskGroupings(id: IdentifierMeaning, rows: DataObject[]): Grouping<SListItem> {
    return id.type === "path"
        ? rows as SListItem[]
        : rows.map(r => iden({
            key: r[id.name],
            rows: extractTaskGroupings(id.on, r.rows as DataObject[]),
        }));
}

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

export interface CalendarExecution {
    core: CoreExecution;
    data: { date: DateTime; link: Link; value?: Literal[] }[];
}

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
