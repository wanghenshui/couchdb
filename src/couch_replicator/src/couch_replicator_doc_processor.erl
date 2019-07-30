% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_replicator_doc_processor).

-behaviour(gen_server).

-export([
    start_link/0
]).

-export([
   init/1,
   terminate/2,
   handle_call/3,
   handle_info/2,
   handle_cast/2,
   code_change/3
]).

-export([
    during_doc_update/3,
    after_db_create/1,
    after_db_delete/1
]).

-export([
    docs/1,
    doc/2,
    doc_lookup/3,
    update_docs/0,
    get_worker_ref/1
]).

-include_lib("couch/include/couch_db.hrl").
-include("couch_replicator.hrl").
-include_lib("mem3/include/mem3.hrl").

-import(couch_replicator_utils, [
    get_json_value/2,
    get_json_value/3
]).

-define(DEFAULT_UPDATE_DOCS, false).
-define(ERROR_MAX_BACKOFF_EXPONENT, 12).  % ~ 1 day on average
-define(TS_DAY_SEC, 86400).
-define(INITIAL_BACKOFF_EXPONENT, 64).
-define(MIN_FILTER_DELAY_SEC, 60).

-type filter_type() ::  nil | view | user | docids | mango.
-type repstate() :: initializing | error | scheduled.


-record(rdoc, {
    id :: db_doc_id() | '_' | {any(), '_'},
    state :: repstate() | '_',
    rep :: #rep{} | nil | '_',
    rid :: rep_id() | nil | '_',
    filter :: filter_type() | '_',
    info :: binary() | nil | '_',
    errcnt :: non_neg_integer() | '_',
    worker :: reference() | nil | '_',
    last_updated :: erlang:timestamp() | '_'
}).


during_doc_update(#doc{} = Doc, Db, _UpdateType) ->
    couch_stats:increment_counter([couch_replicator, docs, db_changes]),
    ok = process_change(Db, Doc).

after_db_create(#{name := DbName}) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_created]),
    couch_replicator_docs:ensure_rep_ddoc_exists(DbName).


after_db_delete(#{name := DbName}) ->
    couch_stats:increment_counter([couch_replicator, docs, dbs_deleted]),
    remove_replications_by_dbname(DbName).


process_change(_Db, #doc{id = <<?DESIGN_DOC_PREFIX, _/binary>>}) ->
    ok;

process_change(#{name := DbName} = Db, #doc{deleted = true} = Doc) ->
    Id = docs_job_id(DbName, Doc#doc.id),
    ok = remove_replication_by_doc_job_id(Db, Id);

process_change(#{name := DbName} = Db, #doc{} = Doc) ->
    #doc{id = DocId, body = {Props} = Body} = Doc,
    {Rep, RepError} = try
        Rep0 = couch_replicator_docs:parse_rep_doc_without_id(Body),
        Rep1 = Rep0#{
            <<"db_name">> => DbName,
            <<"start_time">> => erlang:system_time()
        },
        {Rep1, null}
    catch
        throw:{bad_rep_doc, Reason} ->
            {null, couch_replicator_utils:rep_error_to_binary(Reason)}
    end,
    % We keep track of the doc's state in order to clear it if update_docs
    % is toggled from true to false
    DocState = get_json_value(<<"_replication_state">>, Props, null),
    case couch_jobs:get_job_data(Db, ?REP_DOCS, docs_job_id(DbName, DocId)) of
        {error, not_found} ->
            update_replication_job(Db, DbName, DocId, Rep, RepError, DocState);
        {ok, #{<<"rep">> := null, <<"rep_parse_error">> := RepError}}
                when Rep =:= null ->
            % Same error as before occurred, don't bother updating the job
            ok;
        {ok, #{<<"rep">> := null}} when Rep =:= null ->
            % Error occured but it's a different error. Update the job so user
            % sees the new error
            update_replication_job(Db, DbName, DocId, Rep, RepError, DocState);
        {ok, #{<<"rep">> := OldRep, <<"rep_parse_error">> := OldError}} ->
            NormOldRep = couch_replicator_util:normalize_rep(OldRep),
            NormRep = couch_replicator_util:normalize_rep(Rep),
            case NormOldRep == NormRep of
                true ->
                    % Document was changed but none of the parameters relevent
                    % for the replication job have changed, so make it a no-op
                    ok;
                false ->
                    update_replication_job(Db, DbName, DocId, Rep, RepError,
                        DocState)
            end
    end.


rep_docs_job_execute(#{} = Job, #{<<"rep">> := null} = JobData) ->
    #{
        <<"rep_parse_error">> := Error,
        <<"db_name">> := DbName,
        <<"doc_id">> := DocId,
    } = JobData,
    JobData1 = JobData#{
        <<"finished_state">> := <<"failed">>,
        <<"finished_result">> := Error
    }
    case couch_jobs:finish(undefined, Job, JobData1) of
        ok ->
            couch_replicator_docs:update_failed(DbName, DocId, Error),
            ok;
        {error, JobError} ->
            Msg = "Replication ~s job could not finish. JobError:~p",
            couch_log:error(Msg, [RepId, JobError]),
            {error, JobError}
    end;

rep_docs_job_execute(#{} = Job, #{} = JobData) ->
    #{<<"rep">> := Rep, <<"doc_state">> := DocState} = JobData,
    case lists:member(DocState, [<<"triggered">>, <<"error">>]) of
        true -> maybe_remove_state_fields(DbName, DocId),
        false -> ok
    end,
    % completed jobs should finish right away

    % otherwise start computing the replication id

    Rep1 = update_replication_id(Rep),

    % when done add or update the replicaton job
    % if jobs has a filter keep checking if filter changes


maybe_remove_state_fields(DbName, DocId) ->
    case update_docs() of
        true ->
            ok;
        false ->
            couch_replicator_docs:remove_state_fields(DbName, DocId)
    end.


process_updated({DbName, _DocId} = Id, JsonRepDoc) ->
    % Parsing replication doc (but not calculating the id) could throw an
    % exception which would indicate this document is malformed. This exception
    % should propagate to db_change function and will be recorded as permanent
    % failure in the document. User will have to update the documet to fix the
    % problem.
    Rep0 = couch_replicator_docs:parse_rep_doc_without_id(JsonRepDoc),
    Rep = Rep0#rep{db_name = DbName, start_time = os:timestamp()},
    Filter = case couch_replicator_filters:parse(Rep#rep.options) of
    {ok, nil} ->
        nil;
    {ok, {user, _FName, _QP}} ->
        user;
    {ok, {view, _FName, _QP}} ->
        view;
    {ok, {docids, _DocIds}} ->
        docids;
    {ok, {mango, _Selector}} ->
        mango;
    {error, FilterError} ->
        throw(FilterError)
    end,
    gen_server:call(?MODULE, {updated, Id, Rep, Filter}, infinity).


% Doc processor gen_server API and callbacks

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [],  []).


init([]) ->
    ?MODULE = ets:new(?MODULE, [named_table, {keypos, #rdoc.id},
        {read_concurrency, true}, {write_concurrency, true}]),
    {ok, nil}.


terminate(_Reason, _State) ->
    ok.


handle_call({updated, Id, Rep, Filter}, _From, State) ->
    ok = updated_doc(Id, Rep, Filter),
    {reply, ok, State};

handle_call({removed, Id}, _From, State) ->
    ok = removed_doc(Id),
    {reply, ok, State};

handle_call({completed, Id}, _From, State) ->
    true = ets:delete(?MODULE, Id),
    {reply, ok, State};

handle_call({clean_up_replications, DbName}, _From, State) ->
    ok = removed_db(DbName),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    {stop, {error, unexpected_message, Msg}, State}.


handle_info({'DOWN', _, _, _, #doc_worker_result{id = Id, wref = Ref,
        result = Res}}, State) ->
    ok = worker_returned(Ref, Id, Res),
    {noreply, State};

handle_info(_Msg, State) ->
    {noreply, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


% Doc processor gen_server private helper functions

% Handle doc update -- add to ets, then start a worker to try to turn it into
% a replication job. In most cases it will succeed quickly but for filtered
% replications or if there are duplicates, it could take longer
% (theoretically indefinitely) until a replication could be started. Before
% adding replication job, make sure to delete all old jobs associated with
% same document.
-spec updated_doc(db_doc_id(), #rep{}, filter_type()) -> ok.
updated_doc(Id, Rep, Filter) ->
    NormCurRep = couch_replicator_utils:normalize_rep(current_rep(Id)),
    NormNewRep = couch_replicator_utils:normalize_rep(Rep),
    case NormCurRep == NormNewRep of
        false ->
            removed_doc(Id),
            Row = #rdoc{
                id = Id,
                state = initializing,
                rep = Rep,
                rid = nil,
                filter = Filter,
                info = nil,
                errcnt = 0,
                worker = nil,
                last_updated = os:timestamp()
            },
            true = ets:insert(?MODULE, Row),
            ok = maybe_start_worker(Id);
        true ->
            ok
    end.


% Return current #rep{} record if any. If replication hasn't been submitted
% to the scheduler yet, #rep{} record will be in the document processor's
% ETS table, otherwise query scheduler for the #rep{} record.
-spec current_rep({binary(), binary()}) -> #rep{} | nil.
current_rep({DbName, DocId}) when is_binary(DbName), is_binary(DocId) ->
    case ets:lookup(?MODULE, {DbName, DocId}) of
        [] ->
            nil;
        [#rdoc{state = scheduled, rep = nil, rid = JobId}] ->
            % When replication is scheduled, #rep{} record which can be quite
            % large compared to other bits in #rdoc is removed in order to avoid
            % having to keep 2 copies of it. So have to fetch it from the
            % scheduler.
            couch_replicator_scheduler:rep_state(JobId);
        [#rdoc{rep = Rep}] ->
            Rep
    end.


-spec worker_returned(reference(), db_doc_id(), rep_start_result()) -> ok.
worker_returned(Ref, Id, {ok, RepId}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref} = Row] ->
        Row0 = Row#rdoc{
            state = scheduled,
            errcnt = 0,
            worker = nil,
            last_updated = os:timestamp()
        },
        NewRow = case Row0 of
            #rdoc{rid = RepId, filter = user} ->
                % Filtered replication id didn't change.
                Row0;
            #rdoc{rid = nil, filter = user} ->
                % Calculated new replication id for a filtered replication. Make
                % sure to schedule another check as filter code could change.
                % Replication starts could have been failing, so also clear
                % error count.
                Row0#rdoc{rid = RepId};
            #rdoc{rid = OldRepId, filter = user} ->
                % Replication id of existing replication job with filter has
                % changed. Remove old replication job from scheduler and
                % schedule check to check for future changes.
                ok = couch_replicator_scheduler:remove_job(OldRepId),
                Msg = io_lib:format("Replication id changed: ~p -> ~p", [
                    OldRepId, RepId]),
                Row0#rdoc{rid = RepId, info = couch_util:to_binary(Msg)};
            #rdoc{rid = nil} ->
                % Calculated new replication id for non-filtered replication.
                % Remove replication doc body, after this we won't need it
                % anymore.
                Row0#rdoc{rep=nil, rid=RepId, info=nil}
        end,
        true = ets:insert(?MODULE, NewRow),
        ok = maybe_update_doc_triggered(Row#rdoc.rep, RepId),
        ok = maybe_start_worker(Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok;

worker_returned(_Ref, _Id, ignore) ->
    ok;

worker_returned(Ref, Id, {temporary_error, Reason}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref, errcnt = ErrCnt} = Row] ->
        NewRow = Row#rdoc{
            rid = nil,
            state = error,
            info = Reason,
            errcnt = ErrCnt + 1,
            worker = nil,
            last_updated = os:timestamp()
        },
        true = ets:insert(?MODULE, NewRow),
        ok = maybe_update_doc_error(NewRow#rdoc.rep, Reason),
        ok = maybe_start_worker(Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok;

worker_returned(Ref, Id, {permanent_failure, _Reason}) ->
    case ets:lookup(?MODULE, Id) of
    [#rdoc{worker = Ref}] ->
        true = ets:delete(?MODULE, Id);
    _ ->
        ok  % doc could have been deleted, ignore
    end,
    ok.


-spec maybe_update_doc_error(#rep{}, any()) -> ok.
maybe_update_doc_error(Rep, Reason) ->
    case update_docs() of
        true ->
            couch_replicator_docs:update_error(Rep, Reason);
        false ->
            ok
    end.


-spec maybe_update_doc_triggered(#rep{}, rep_id()) -> ok.
maybe_update_doc_triggered(Rep, RepId) ->
    case update_docs() of
        true ->
            couch_replicator_docs:update_triggered(Rep, RepId);
        false ->
            ok
    end.


-spec error_backoff(non_neg_integer()) -> seconds().
error_backoff(ErrCnt) ->
    Exp = min(ErrCnt, ?ERROR_MAX_BACKOFF_EXPONENT),
    % ErrCnt is the exponent here. The reason 64 is used is to start at
    % 64 (about a minute) max range. Then first backoff would be 30 sec
    % on average. Then 1 minute and so on.
    couch_rand:uniform(?INITIAL_BACKOFF_EXPONENT bsl Exp).


-spec filter_backoff() -> seconds().
filter_backoff() ->
    Total = ets:info(?MODULE, size),
    % This value scaled by the number of replications. If the are a lot of them
    % wait is longer, but not more than a day (?TS_DAY_SEC). If there are just
    % few, wait is shorter, starting at about 30 seconds. `2 *` is used since
    % the expected wait would then be 0.5 * Range so it is easier to see the
    % average wait. `1 +` is used because couch_rand:uniform only
    % accepts >= 1 values and crashes otherwise.
    Range = 1 + min(2 * (Total / 10), ?TS_DAY_SEC),
    ?MIN_FILTER_DELAY_SEC + couch_rand:uniform(round(Range)).


% Document removed from db -- clear ets table and remove all scheduled jobs
-spec removed_doc(db_doc_id()) -> ok.
removed_doc({DbName, DocId} = Id) ->
    ets:delete(?MODULE, Id),
    RepIds = couch_replicator_scheduler:find_jobs_by_doc(DbName, DocId),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


% Whole db shard is gone -- remove all its ets rows and stop jobs
-spec removed_db(binary()) -> ok.
removed_db(DbName) ->
    EtsPat = #rdoc{id = {DbName, '_'}, _ = '_'},
    ets:match_delete(?MODULE, EtsPat),
    RepIds = couch_replicator_scheduler:find_jobs_by_dbname(DbName),
    lists:foreach(fun couch_replicator_scheduler:remove_job/1, RepIds).


% Spawn a worker process which will attempt to calculate a replication id, then
% start a replication. Returns a process monitor reference. The worker is
% guaranteed to exit with rep_start_result() type only.
-spec maybe_start_worker(db_doc_id()) -> ok.
maybe_start_worker(Id) ->
    case ets:lookup(?MODULE, Id) of
    [] ->
        ok;
    [#rdoc{state = scheduled, filter = Filter}] when Filter =/= user ->
        ok;
    [#rdoc{rep = Rep} = Doc] ->
        % For any replication with a user created filter function, periodically
        % (every `filter_backoff/0` seconds) to try to see if the user filter
        % has changed by using a worker to check for changes. When the worker
        % returns check if replication ID has changed. If it hasn't keep
        % checking (spawn another worker and so on). If it has stop the job
        % with the old ID and continue checking.
        Wait = get_worker_wait(Doc),
        Ref = make_ref(),
        true = ets:insert(?MODULE, Doc#rdoc{worker = Ref}),
        couch_replicator_doc_processor_worker:spawn_worker(Id, Rep, Wait, Ref),
        ok
    end.


-spec get_worker_wait(#rdoc{}) -> seconds().
get_worker_wait(#rdoc{state = scheduled, filter = user}) ->
    filter_backoff();
get_worker_wait(#rdoc{state = error, errcnt = ErrCnt}) ->
    error_backoff(ErrCnt);
get_worker_wait(#rdoc{state = initializing}) ->
    0.


-spec update_docs() -> boolean().
update_docs() ->
    config:get_boolean("replicator", "update_docs", ?DEFAULT_UPDATE_DOCS).


% _scheduler/docs HTTP endpoint helpers

-spec docs([atom()]) -> [{[_]}] | [].
docs(States) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    ets:foldl(fun(RDoc, Acc) ->
        case ejson_doc(RDoc, HealthThreshold) of
            nil ->
                Acc;  % Could have been deleted if job just completed
            {Props} = EJson ->
                {state, DocState} = lists:keyfind(state, 1, Props),
                case ejson_doc_state_filter(DocState, States) of
                    true ->
                        [EJson | Acc];
                    false ->
                        Acc
                end
        end
    end, [], ?MODULE).


-spec doc(binary(), binary()) -> {ok, {[_]}} | {error, not_found}.
doc(Db, DocId) ->
    HealthThreshold = couch_replicator_scheduler:health_threshold(),
    Res = (catch ets:foldl(fun(RDoc, nil) ->
        {Shard, RDocId} = RDoc#rdoc.id,
        case {mem3:dbname(Shard), RDocId} of
            {Db, DocId} ->
                throw({found, ejson_doc(RDoc, HealthThreshold)});
            {_OtherDb, _OtherDocId} ->
                nil
        end
    end, nil, ?MODULE)),
    case Res of
        {found, DocInfo} ->
            {ok, DocInfo};
        nil ->
            {error, not_found}
    end.


-spec doc_lookup(binary(), binary(), integer()) ->
    {ok, {[_]}} | {error, not_found}.
doc_lookup(Db, DocId, HealthThreshold) ->
    case ets:lookup(?MODULE, {Db, DocId}) of
        [#rdoc{} = RDoc] ->
            {ok, ejson_doc(RDoc, HealthThreshold)};
        [] ->
            {error, not_found}
    end.


-spec ejson_state_info(binary() | nil) -> binary() | null.
ejson_state_info(nil) ->
    null;
ejson_state_info(Info) when is_binary(Info) ->
    Info;
ejson_state_info(Info) ->
    couch_replicator_utils:rep_error_to_binary(Info).


-spec ejson_rep_id(rep_id() | nil) -> binary() | null.
ejson_rep_id(nil) ->
    null;
ejson_rep_id({BaseId, Ext}) ->
    iolist_to_binary([BaseId, Ext]).


-spec ejson_doc(#rdoc{}, non_neg_integer()) -> {[_]} | nil.
ejson_doc(#rdoc{state = scheduled} = RDoc, HealthThreshold) ->
    #rdoc{id = {DbName, DocId}, rid = RepId} = RDoc,
    JobProps = couch_replicator_scheduler:job_summary(RepId, HealthThreshold),
    case JobProps of
        nil ->
            nil;
        [{_, _} | _] ->
            {[
                {doc_id, DocId},
                {database, DbName},
                {id, ejson_rep_id(RepId)},
                {node, node()} | JobProps
            ]}
    end;

ejson_doc(#rdoc{state = RepState} = RDoc, _HealthThreshold) ->
    #rdoc{
       id = {DbName, DocId},
       info = StateInfo,
       rid = RepId,
       errcnt = ErrorCount,
       last_updated = StateTime,
       rep = Rep
    } = RDoc,
    {[
        {doc_id, DocId},
        {database, DbName},
        {id, ejson_rep_id(RepId)},
        {state, RepState},
        {info, ejson_state_info(StateInfo)},
        {error_count, ErrorCount},
        {node, node()},
        {last_updated, couch_replicator_utils:iso8601(StateTime)},
        {start_time, couch_replicator_utils:iso8601(Rep#rep.start_time)}
    ]}.


-spec ejson_doc_state_filter(atom(), [atom()]) -> boolean().
ejson_doc_state_filter(_DocState, []) ->
    true;
ejson_doc_state_filter(State, States) when is_list(States), is_atom(State) ->
    lists:member(State, States).


-spec update_replication(any(), binary(), binary(), #{} | null,
    binary() | null, binary() | null) -> ok.
update_replication_job(Tx, DbName, DocId, Rep, RepParseError, DocState) ->
    JobId = docs_job_id(DbName, DocId),
    ok = remove_replication_by_doc_job_id(Db, JobId),
    RepDocsJob = #{
        <<"rep_id">> := null,
        <<"db_name">> := DbName,
        <<"doc_id">> := DocId,
        <<"rep">> := Rep,
        <<"rep_parse_error">> := RepParseError,
        <<"doc_state">> := DocState
    },
    ok = couch_jobs:add(Tx, ?REP_DOCS, RepDocsJob).


docs_job_id(DbName, Id) when is_binary(DbName), is_binary(Id) ->
    <<DbName/binary, "|", Id/binary>>.


-spec remove_replication_by_doc_job_id(Tx, Id) -> ok.
remove_replication_by_doc_job_id(Tx, Id) ->
    case couch_jobs:get_job_data(Tx, ?REP_DOCS, Id) of
        {error, not_found} ->
            ok;
        {ok, #{<<"rep_id">> := null}} ->
            couch_jobs:remove(Tx, ?REP_DOCS, Id),
            ok;
        {ok, #{<<"rep_id">> := RepId}} ->
            couch_jobs:remove(Tx, ?REP_JOBS, RepId),
            couch_jobs:remove(Tx, ?REP_DOCS, Id),
            ok
    end.


-spec remove_replications_by_dbname(DbName) -> ok.
remove_replications_by_dbname(DbName) ->
    DbNameSize = byte_size(DbName),
    Filter = fun
        (<<DbName:DbNameSize/binary, "|", _, _/binary>>) -> true;
        (_) -> false
    end,
    JobsMap = couch_job:get_jobs(undefined, ?REP_DOCS, Filter),
    % Batch these into smaller transactions eventually...
    couch_jobs_fdb:tx(couch_jobs_fdb:get_jtx(Tx), fun(JTx) ->
        maps:map(fun(Id, _) ->
            remove_replication_by_doc_job_id(JTx, Id)
        end, JobsMap)
    end).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-define(DB, <<"db">>).
-define(DOC1, <<"doc1">>).
-define(DOC2, <<"doc2">>).
-define(R1, {"1", ""}).
-define(R2, {"2", ""}).


doc_processor_test_() ->
    {
        foreach,
        fun setup/0,
        fun teardown/1,
        [
            t_bad_change(),
            t_regular_change(),
            t_change_with_existing_job(),
            t_deleted_change(),
            t_triggered_change(),
            t_completed_change(),
            t_active_replication_completed(),
            t_error_change(),
            t_failed_change(),
            t_change_for_different_node(),
            t_change_when_cluster_unstable(),
            t_ejson_docs()
        ]
    }.


% Can't parse replication doc, so should write failure state to document.
t_bad_change() ->
    ?_test(begin
        ?assertEqual(acc, db_change(?DB, bad_change(), acc)),
        ?assert(updated_doc_with_failed_state())
    end).


% Regular change, parse to a #rep{} and then add job.
t_regular_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Regular change, parse to a #rep{} and then add job but there is already
% a running job with same Id found.
t_change_with_existing_job() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is a deletion, and job is running, so remove job.
t_deleted_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([test_rep(?R2)]),
        ?assertEqual(ok, process_change(?DB, deleted_change())),
        ?assert(removed_job(?R2))
    end).


% Change is in `triggered` state. Remove legacy state and add job.
t_triggered_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"triggered">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `completed` state, so skip over it.
t_completed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Completed change comes for what used to be an active job. In this case
% remove entry from doc_processor's ets (because there is no linkage or
% callback mechanism for scheduler to tell doc_processsor a replication just
% completed).
t_active_replication_completed() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assertEqual(ok, process_change(?DB, change(<<"completed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1}))
    end).


% Change is in `error` state. Remove legacy state and retry
% running the job. This state was used for transient erorrs which are not
% written to the document anymore.
t_error_change() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change(<<"error">>))),
        ?assert(removed_state_fields()),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(started_worker({?DB, ?DOC1}))
    end).


% Change is in `failed` state. This is a terminal state and it will not
% be tried again, so skip over it.
t_failed_change() ->
    ?_test(begin
        ?assertEqual(ok, process_change(?DB, change(<<"failed">>))),
        ?assert(did_not_remove_state_fields()),
        ?assertNot(ets:member(?MODULE, {?DB, ?DOC1})),
        ?assert(did_not_spawn_worker())
    end).


% Normal change, but according to cluster ownership algorithm, replication
% belongs to a different node, so this node should skip it.
t_change_for_different_node() ->
   ?_test(begin
        meck:expect(couch_replicator_clustering, owner, 2, different_node),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(did_not_spawn_worker())
   end).


% Change handled when cluster is unstable (nodes are added or removed), so
% job is not added. A rescan will be triggered soon and change will be
% evaluated again.
t_change_when_cluster_unstable() ->
   ?_test(begin
       meck:expect(couch_replicator_clustering, owner, 2, unstable),
       ?assertEqual(ok, process_change(?DB, change())),
       ?assert(did_not_spawn_worker())
   end).


% Check if docs/0 function produces expected ejson after adding a job
t_ejson_docs() ->
    ?_test(begin
        mock_existing_jobs_lookup([]),
        ?assertEqual(ok, process_change(?DB, change())),
        ?assert(ets:member(?MODULE, {?DB, ?DOC1})),
        EJsonDocs = docs([]),
        ?assertMatch([{[_|_]}], EJsonDocs),
        [{DocProps}] = EJsonDocs,
        {value, StateTime, DocProps1} = lists:keytake(last_updated, 1,
            DocProps),
        ?assertMatch({last_updated, BinVal1} when is_binary(BinVal1),
            StateTime),
        {value, StartTime, DocProps2} = lists:keytake(start_time, 1, DocProps1),
        ?assertMatch({start_time, BinVal2} when is_binary(BinVal2), StartTime),
        ExpectedProps = [
            {database, ?DB},
            {doc_id, ?DOC1},
            {error_count, 0},
            {id, null},
            {info, null},
            {node, node()},
            {state, initializing}
        ],
        ?assertEqual(ExpectedProps, lists:usort(DocProps2))
    end).


get_worker_ref_test_() ->
    {
        setup,
        fun() ->
            ets:new(?MODULE, [named_table, public, {keypos, #rdoc.id}])
        end,
        fun(_) -> ets:delete(?MODULE) end,
        ?_test(begin
            Id = {<<"db">>, <<"doc">>},
            ?assertEqual(nil, get_worker_ref(Id)),
            ets:insert(?MODULE, #rdoc{id = Id, worker = nil}),
            ?assertEqual(nil, get_worker_ref(Id)),
            Ref = make_ref(),
            ets:insert(?MODULE, #rdoc{id = Id, worker = Ref}),
            ?assertEqual(Ref, get_worker_ref(Id))
        end)
    }.


% Test helper functions


setup() ->
    meck:expect(couch_log, info, 2, ok),
    meck:expect(couch_log, notice, 2, ok),
    meck:expect(couch_log, warning, 2, ok),
    meck:expect(couch_log, error, 2, ok),
    meck:expect(config, get, fun(_, _, Default) -> Default end),
    meck:expect(config, listen_for_changes, 2, ok),
    meck:expect(couch_replicator_clustering, owner, 2, node()),
    meck:expect(couch_replicator_clustering, link_cluster_event_listener, 3,
        ok),
    meck:expect(couch_replicator_doc_processor_worker, spawn_worker, 4, pid),
    meck:expect(couch_replicator_scheduler, remove_job, 1, ok),
    meck:expect(couch_replicator_docs, remove_state_fields, 2, ok),
    meck:expect(couch_replicator_docs, update_failed, 3, ok),
    {ok, Pid} = start_link(),
    Pid.


teardown(Pid) ->
    unlink(Pid),
    exit(Pid, kill),
    meck:unload().


removed_state_fields() ->
    meck:called(couch_replicator_docs, remove_state_fields, [?DB, ?DOC1]).


started_worker(_Id) ->
    1 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker, 4).


removed_job(Id) ->
    meck:called(couch_replicator_scheduler, remove_job, [test_rep(Id)]).


did_not_remove_state_fields() ->
    0 == meck:num_calls(couch_replicator_docs, remove_state_fields, '_').


did_not_spawn_worker() ->
    0 == meck:num_calls(couch_replicator_doc_processor_worker, spawn_worker,
        '_').

updated_doc_with_failed_state() ->
    1 == meck:num_calls(couch_replicator_docs, update_failed, '_').


mock_existing_jobs_lookup(ExistingJobs) ->
    meck:expect(couch_replicator_scheduler, find_jobs_by_doc,
        fun(?DB, ?DOC1) -> ExistingJobs end).


test_rep(Id) ->
  #rep{id = Id, start_time = {0, 0, 0}}.


change() ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


change(State) ->
    {[
        {<<"id">>, ?DOC1},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>},
            {<<"_replication_state">>, State}
        ]}}
    ]}.


deleted_change() ->
    {[
        {<<"id">>, ?DOC1},
        {<<"deleted">>, true},
        {doc, {[
            {<<"_id">>, ?DOC1},
            {<<"source">>, <<"src">>},
            {<<"target">>, <<"tgt">>}
        ]}}
    ]}.


bad_change() ->
    {[
        {<<"id">>, ?DOC2},
        {doc, {[
            {<<"_id">>, ?DOC2},
            {<<"source">>, <<"src">>}
        ]}}
    ]}.

-endif.
