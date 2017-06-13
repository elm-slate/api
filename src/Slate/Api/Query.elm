module Slate.Api.Query
    exposing
        ( Config
        , Model
        , Msg(..)
        , QueryError
        , QueryResult
        , ExecutionId
        , ApiExecutionState
        , ApiQueryState
        , ApiQueryResultTagger
        , FetchFunctionReturn
        , FetchAndWatchFunctionReturn
        , init
        , start
        , stop
        , update
        , subscriptions
        , queryComplete
        , getQueryState
        , setQueryState
        , getCurrentExecutionId
        , getCurrentExecutionState
        , getCurrentMutationState
        , setCurrentMutationState
        , fetch
        , fetchAndWatch
        , unwatch
        , unwatchAll
        )

{-|
    Common Query code for creating Query APIs.

@docs Config, Model, Msg, QueryError, QueryResult, ExecutionId, ApiExecutionState, ApiQueryState, ApiQueryResultTagger, FetchFunctionReturn, FetchAndWatchFunctionReturn, init, start, stop, update, subscriptions, queryComplete, getQueryState, setQueryState, getCurrentExecutionId, getCurrentExecutionState, getCurrentMutationState, setCurrentMutationState, fetch, fetchAndWatch, unwatch, unwatchAll
-}

import Time exposing (Time)
import Task
import Process
import Dict as Dict exposing (Dict)
import Maybe.Extra as Maybe
import List.Extra as List
import StringUtils exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Engine.Query as Query exposing (..)
import Slate.Engine.Engine as Engine exposing (..)
import Slate.Common.Taggers exposing (..)
import Slate.DbWatcher as DbWatcher exposing (..)
import ParentChildUpdate exposing (..)
import EventEmitter as EventEmitter
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Utils.Ops exposing (..)


{-| Unique execution id for queries
-}
type alias ExecutionId =
    EventEmitter.Id


{-| A single query execution state
-}
type alias ApiExecutionState mutationState =
    { executionCount : Int
    , mutationState : mutationState
    }


type alias ExecutionStates mutationState =
    Dict ExecutionId (ApiExecutionState mutationState)


{-| Query Error.
-}
type alias QueryError =
    ( ErrorType, String )


{-| State per query
-}
type alias ApiQueryState mutationState =
    { persist : Bool
    , currentExecutionId : Maybe ExecutionId
    , executionStates : ExecutionStates mutationState
    , maybeError : Maybe QueryError
    }


{-| Module's config
-}
type alias Config mutation mutationState completionTagger msg =
    { routeToMeTagger : Msg mutation completionTagger -> msg
    , dbWatcherReconnectDelayInterval : Time
    , dbWatcherStopDelayInterval : Time
    , returnCmds : List (Cmd msg) -> msg
    , logTagger : LogTagger String msg
    , errorTagger : ErrorTagger String msg
    , onError : completionTagger -> QueryError -> msg
    , onComplete : ApiExecutionState mutationState -> completionTagger -> Maybe (List PropertyName) -> msg
    , onMutate : Model mutation mutationState completionTagger msg -> QueryId -> mutation -> ( ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ), List msg )
    , debug : Bool
    }


{-| Query Result
-}
type alias QueryResult =
    Result QueryError ()


{-| Fetch function return type for API developers
-}
type alias FetchFunctionReturn mutation mutationState completionTagger msg =
    ( Query (Msg mutation completionTagger), ( QueryId, Model mutation mutationState completionTagger msg, Cmd msg ) )


type alias FetchFunction mutation mutationState completionTagger msg =
    Model mutation mutationState completionTagger msg -> FetchFunctionReturn mutation mutationState completionTagger msg


{-| Fetch and Watch function return type for API developers
-}
type alias FetchAndWatchFunctionReturn mutation mutationState completionTagger msg =
    ( Query (Msg mutation completionTagger), ( QueryId, Model mutation mutationState completionTagger msg, Cmd msg ) )


type alias FetchAndWatchFunction mutation mutationState completionTagger msg =
    Model mutation mutationState completionTagger msg -> FetchAndWatchFunctionReturn mutation mutationState completionTagger msg


type FetchOperation mutation mutationState completionTagger msg
    = Fetch (FetchFunction mutation mutationState completionTagger msg)


type alias ApiName =
    String


type alias Watch mutation mutationState completionTagger msg =
    ( msg, ApiName, FetchOperation mutation mutationState completionTagger msg, completionTagger )


{-| Modules model
-}
type alias Model mutation mutationState completionTagger msg =
    { fullyQualifiedModuleName : String
    , nextExecutionId : Int
    , queryStates : Dict QueryId (ApiQueryState mutationState)
    , engineModel : Engine.Model (Msg mutation completionTagger)
    , dbWatcherModel : DbWatcher.Model
    , persitentQueryIds : Dict String QueryId
    , watches : Dict QueryId (List (Watch mutation mutationState completionTagger msg))
    , refetchQueryId : Maybe QueryId
    , started : Bool
    , dbConnectionInfo : Maybe DbConnectionInfo
    }


engineConfig : Config mutation mutationState completionTagger msg -> Engine.Config (Msg mutation completionTagger)
engineConfig config =
    { debug = config.debug
    , logTagger = EngineLog
    , errorTagger = EngineError
    , eventProcessingErrorTagger = EventProcessingError
    , completionTagger = EventProcessingComplete
    , routeToMeTagger = EngineMsg
    , queryBatchSize = 2000
    }


dbWatcherConfig : Config mutation mutationState completionTagger msg -> DbWatcher.Config (Msg mutation completionTagger)
dbWatcherConfig config =
    { pgReconnectDelayInterval = config.dbWatcherReconnectDelayInterval
    , stopDelayInterval = config.dbWatcherStopDelayInterval
    , errorTagger = DbWatcherError
    , logTagger = DbWatcherLog
    , routeToMeTagger = DbWatcherMsg
    , refreshTagger = DbWatcherRefreshRequired
    , debug = config.debug
    }


initModel :
    Config mutation mutationState completionTagger msg
    -> DbConnectionInfo
    -> String
    -> ( Model mutation mutationState completionTagger msg, List (Cmd (Msg mutation completionTagger)) )
initModel config dbConnectionInfo fullyQualifiedModuleName =
    let
        ( engineModel, engineCmd ) =
            Engine.init (engineConfig config)

        ( dbWatcherModel, dbWatcherCmd ) =
            DbWatcher.init (dbWatcherConfig config)
    in
        ( { fullyQualifiedModuleName = fullyQualifiedModuleName
          , nextExecutionId = 0
          , queryStates = Dict.empty
          , engineModel = engineModel
          , dbWatcherModel = dbWatcherModel
          , persitentQueryIds = Dict.empty
          , watches = Dict.empty
          , refetchQueryId = Nothing
          , started = False
          , dbConnectionInfo = Nothing
          }
        , [ engineCmd, dbWatcherCmd ]
        )


{-| Initialize command processor
-}
init : Config mutation mutationState completionTagger msg -> DbConnectionInfo -> String -> ( Model mutation mutationState completionTagger msg, Cmd msg )
init config dbConnectionInfo fullyQualifiedModuleName =
    let
        ( model, cmds ) =
            initModel config dbConnectionInfo fullyQualifiedModuleName
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


{-| Start API
-}
start :
    Config mutation mutationState completionTagger msg
    -> DbConnectionInfo
    -> Model mutation mutationState completionTagger msg
    -> Result String ( Model mutation mutationState completionTagger msg, Cmd msg )
start config dbConnectionInfo model =
    not model.started
        ? ( DbWatcher.start (dbWatcherConfig config) model.dbWatcherModel [ dbConnectionInfo ]
                |> (\( dbWatcherModel, dbWatcherCmd ) -> Ok <| { model | started = True, dbConnectionInfo = Just dbConnectionInfo, dbWatcherModel = dbWatcherModel } ! [ Cmd.map config.routeToMeTagger dbWatcherCmd ])
          , (Err "Already Started")
          )


{-| Stop API
-}
stop :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> ApiName
    -> Result String ( Model mutation mutationState completionTagger msg, Cmd msg )
stop config model apiName =
    (unwatchAll config model apiName)
        |> (\( model, unwatchAllCmd ) ->
                (model.started
                    ? ( DbWatcher.stop (dbWatcherConfig config) model.dbWatcherModel
                            |> (\( dbWatcherModel, dbWatcherCmd ) -> Ok <| { model | started = False, dbConnectionInfo = Nothing, dbWatcherModel = dbWatcherModel } ! [ Cmd.map config.routeToMeTagger dbWatcherCmd, unwatchAllCmd ])
                      , Err "Not started"
                      )
                )
           )


isWatched : Model mutation mutationState completionTagger msg -> QueryId -> Bool
isWatched model queryId =
    Dict.member queryId model.watches


disposeQuery : Model mutation mutationState completionTagger msg -> QueryId -> Model mutation mutationState completionTagger msg
disposeQuery model queryId =
    let
        queryState =
            getQueryState queryId model
    in
        (queryState.persist || isWatched model queryId) ? ( model, { model | engineModel = Engine.disposeQuery model.engineModel queryId, queryStates = Dict.remove queryId model.queryStates } )


countDownExecution : Model mutation mutationState completionTagger msg -> QueryId -> ExecutionId -> Model mutation mutationState completionTagger msg
countDownExecution model queryId executionId =
    let
        executionState =
            getExecutionState queryId executionId model

        queryState =
            getQueryState queryId model
    in
        queryState.persist
            ? ( let
                    executionCount =
                        executionState.executionCount - 1
                in
                    (executionCount == 0)
                        ? ( setQueryState queryId model { queryState | executionStates = Dict.remove executionId queryState.executionStates }
                          , setExecutionState queryId executionId model { executionState | executionCount = executionCount }
                          )
              , disposeQuery model queryId
              )


getMaybeQueryState : QueryId -> Model mutation mutationState completionTagger msg -> Maybe (ApiQueryState mutationState)
getMaybeQueryState queryId model =
    Dict.get queryId model.queryStates


getExecutionState : QueryId -> ExecutionId -> Model mutation mutationState completionTagger msg -> ApiExecutionState mutationState
getExecutionState queryId executionId model =
    let
        queryState =
            getQueryState queryId model
    in
        Dict.get executionId queryState.executionStates ?!= (\_ -> Debug.crash ("Cannot get executionState for queryId:" +-+ queryId +-+ "executionId:" +-+ executionId))


getNextExecutionId : Model mutation mutationState completionTagger msg -> ( Model mutation mutationState completionTagger msg, ExecutionId )
getNextExecutionId model =
    let
        nextExecutionId =
            model.nextExecutionId + 1

        executionId =
            model.fullyQualifiedModuleName ++ (toString nextExecutionId)
    in
        ( { model | nextExecutionId = nextExecutionId }, executionId )


getMaybeCurrentExecutionId : QueryId -> Model mutation mutationState completionTagger msg -> Maybe ExecutionId
getMaybeCurrentExecutionId queryId model =
    Dict.get queryId model.queryStates |?> .currentExecutionId ?= Nothing


setExecutionState : QueryId -> ExecutionId -> Model mutation mutationState completionTagger msg -> ApiExecutionState mutationState -> Model mutation mutationState completionTagger msg
setExecutionState queryId executionId model executionState =
    let
        queryState =
            getQueryState queryId model
    in
        setQueryState queryId model { queryState | executionStates = Dict.insert executionId executionState queryState.executionStates }


executeQuery : Config mutation mutationState completionTagger msg -> DbConnectionInfo -> Model mutation mutationState completionTagger msg -> Query (Msg mutation completionTagger) -> List EntityId -> Bool -> ( QueryId, ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ) )
executeQuery config dbConnectionInfo model query ids persist =
    let
        result =
            Engine.executeQuery (engineConfig config) dbConnectionInfo model.engineModel Nothing query ids
    in
        result
            |??>
                (\( engineModel, cmd, queryId ) ->
                    ( queryId, { model | engineModel = engineModel } ! [ cmd ] )
                )
            ??= (\errors -> Debug.crash <| "Query error:" +-+ (String.join "\n" errors))


refresh : Config mutation mutationState completionTagger msg -> DbConnectionInfo -> Model mutation mutationState completionTagger msg -> QueryId -> ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) )
refresh config dbConnectionInfo model queryId =
    Engine.refreshQuery (engineConfig config) dbConnectionInfo model.engineModel queryId
        |??>
            (\( engineModel, cmd ) ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    { model | engineModel = engineModel } ! [ cmd ]
            )
        ??= (\error -> Debug.crash <| "Refresh error:" +-+ error)


findWatch : QueryId -> msg -> Model mutation mutationState completionTagger msg -> Maybe (Watch mutation mutationState completionTagger msg)
findWatch queryId identityMsg model =
    (Dict.get queryId model.watches)
        |?> List.find (\( msg, _, _, _ ) -> msg == identityMsg)
        ?= Nothing


getDbConnectionInfo : Model mutation mutationState completionTagger msg -> DbConnectionInfo
getDbConnectionInfo model =
    model.dbConnectionInfo ?!= (\_ -> Debug.crash "Query not started")


dbWatch :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> List EntityEventTypes
    -> QueryId
    -> ApiName
    -> FetchFunction mutation mutationState completionTagger msg
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> msg
    -> Result (List String) ( Model mutation mutationState completionTagger msg, Cmd msg )
dbWatch config model queryEventTypes queryId apiName apiFetchFunction queryCompletionTagger completionTagger identityMsg =
    let
        interceptedApiFetchFunction =
            (\model ->
                let
                    ( query, ( _, model2, cmd ) ) =
                        apiFetchFunction { model | refetchQueryId = Just queryId }
                in
                    ( query, ( queryId, { model2 | refetchQueryId = Nothing }, cmd ) )
            )
    in
        findWatch queryId identityMsg model
            |?> (\_ -> Ok <| model ! [])
            ?= (DbWatcher.subscribe (dbWatcherConfig config) (getDbConnectionInfo model) model.dbWatcherModel queryEventTypes queryId
                    |??>
                        (\( dbWatcherModel, dbWatcherCmd ) ->
                            let
                                watch =
                                    ( identityMsg, apiName, Fetch interceptedApiFetchFunction, queryCompletionTagger (completionTagger queryId) ) :: (Dict.get queryId model.watches ?= [])

                                watches =
                                    Dict.insert queryId watch model.watches
                            in
                                { model | watches = watches, dbWatcherModel = dbWatcherModel } ! [ Cmd.map config.routeToMeTagger dbWatcherCmd ]
                        )
               )


sendParentUpdateMsg : msg -> Cmd msg
sendParentUpdateMsg msg =
    Task.perform (\_ -> msg) <| Process.sleep 0



-- API


{-| Module's Msgs
-}
type Msg mutation completionTagger
    = EngineMsg Engine.Msg
    | EngineLog ( LogLevel, ( QueryId, String ) )
    | EngineError ( ErrorType, ( QueryId, String ) )
    | DbWatcherMsg DbWatcher.Msg
    | DbWatcherLog ( LogLevel, String )
    | DbWatcherError ( ErrorType, String )
    | DbWatcherRefreshRequired (List QueryId)
    | Mutate QueryId mutation
    | UnspecifiedMutationInQuery QueryId EventRecord
    | EventError EventRecord ( QueryId, String )
    | EventProcessingError ( String, String )
    | EventProcessingComplete QueryId
    | QueryCompleteEvent completionTagger QueryId (Maybe (List PropertyName)) ExecutionId


{-| Update.
-}
update :
    Config mutation mutationState completionTagger msg
    -> Msg mutation completionTagger
    -> Model mutation mutationState completionTagger msg
    -> ( ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ), List msg )
update config msg model =
    let
        logMsg message =
            config.logTagger ( LogLevelInfo, message )

        updateEngine =
            updateChildParent (Engine.update (engineConfig config)) (update config) .engineModel EngineMsg (\model engineModel -> { model | engineModel = engineModel })

        updateDbWatcher =
            updateChildParent (DbWatcher.update (dbWatcherConfig config)) (update config) .dbWatcherModel DbWatcherMsg (\model dbWatcherModel -> { model | dbWatcherModel = dbWatcherModel })
    in
        case msg of
            EngineMsg msg ->
                updateEngine msg model

            EngineLog ( logLevel, ( queryId, message ) ) ->
                ( model ! [], [ logMsg ("Engine:" +-+ ( queryId, message )) ] )

            EngineError ( errorType, ( queryId, error ) ) ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    (queryComplete model queryId <| Err ( errorType, error ))
                        |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            DbWatcherMsg msg ->
                updateDbWatcher msg model

            DbWatcherLog ( logLevel, details ) ->
                ( model ! [], [ config.logTagger <| ( logLevel, "DbWatcher:" +-+ details ) ] )

            DbWatcherError ( errorType, details ) ->
                case errorType of
                    FatalError ->
                        Debug.crash ("Fatal DbWatcherError:" +-+ details)

                    _ ->
                        ( model ! [], [ config.errorTagger ( errorType, details ) ] )

            DbWatcherRefreshRequired queryIds ->
                let
                    ( newModel, cmds ) =
                        queryIds
                            |> List.filterMap
                                (\queryId ->
                                    Dict.get queryId model.watches
                                        |?> List.map (\( _, _, fetchOperation, _ ) -> fetchOperation)
                                )
                            |> List.concat
                            |> List.foldl
                                (\apiFetchFunction ( model, cmds ) ->
                                    let
                                        ( _, ( _, newModel, cmd ) ) =
                                            case apiFetchFunction of
                                                Fetch fetch ->
                                                    fetch model
                                    in
                                        ( newModel, cmd :: cmds )
                                )
                                ( model, [] )
                in
                    ( newModel ! [], [ config.returnCmds cmds ] )

            UnspecifiedMutationInQuery queryId eventRecord ->
                Debug.crash <| toString ( queryId, "UnspecifiedMutationInQuery:" +-+ eventRecord )

            EventError eventRecord ( queryId, error ) ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    (queryComplete model queryId <| Err ( NonFatalError, "EventError:" +-+ ( queryId, error ) +-+ "for eventRecord:" +-+ eventRecord ))
                        |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            EventProcessingError ( eventStr, error ) ->
                Debug.crash <| "Event Processing Error:" +-+ error +-+ "\nEvent:" +-+ eventStr

            Mutate queryId mutation ->
                config.onMutate model queryId mutation
                    |> (\( ( model, cmd ), msgs ) -> ( model ! [ cmd ], msgs ))

            EventProcessingComplete queryId ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    (queryComplete model queryId <| Ok ())
                        |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            QueryCompleteEvent completionTagger queryId maybeProperties executionId ->
                let
                    queryState =
                        getQueryState queryId model

                    executionState =
                        getExecutionState queryId executionId model

                    msg =
                        queryState.maybeError
                            |?> config.onError completionTagger
                            ?= config.onComplete executionState completionTagger maybeProperties
                in
                    ( countDownExecution model queryId executionId ! [], [ msg ] )


{-| subscriptions
-}
subscriptions :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> Sub msg
subscriptions config model =
    Sub.map config.routeToMeTagger <| DbWatcher.elmSubscriptions (dbWatcherConfig config) model.dbWatcherModel


{-| Get query state for speicified queryId
-}
getQueryState : QueryId -> Model mutation mutationState completionTagger msg -> ApiQueryState mutationState
getQueryState queryId model =
    getMaybeQueryState queryId model ?!= (\_ -> Debug.crash ("Cannot find queryState for queryId:" +-+ queryId))


{-| Set query state for speicified queryId
-}
setQueryState : QueryId -> Model mutation mutationState completionTagger msg -> ApiQueryState mutationState -> Model mutation mutationState completionTagger msg
setQueryState queryId model queryState =
    { model | queryStates = Dict.insert queryId queryState model.queryStates }


{-| Get current execution id for specified queryId
-}
getCurrentExecutionId : QueryId -> Model mutation mutationState completionTagger msg -> ExecutionId
getCurrentExecutionId queryId model =
    getMaybeCurrentExecutionId queryId model ?!= (\_ -> Debug.crash ("Cannot get currentExecutionId for queryId:" +-+ queryId))


{-| Get current execution state for specified queryId
-}
getCurrentExecutionState : QueryId -> Model mutation mutationState completionTagger msg -> ApiExecutionState mutationState
getCurrentExecutionState queryId model =
    let
        executionId =
            getCurrentExecutionId queryId model
    in
        getExecutionState queryId executionId model


{-| Get current Entity MutationState for specified queryId
-}
getCurrentMutationState : QueryId -> Model mutation mutationState completionTagger msg -> mutationState
getCurrentMutationState queryId model =
    let
        executionId =
            getCurrentExecutionId queryId model
    in
        getExecutionState queryId executionId model
            |> .mutationState


{-| Set current Entity MutationState for specified queryId
-}
setCurrentMutationState : QueryId -> Model mutation mutationState completionTagger msg -> mutationState -> Model mutation mutationState completionTagger msg
setCurrentMutationState queryId model mutationState =
    let
        executionId =
            getCurrentExecutionId queryId model

        executionState =
            getExecutionState queryId executionId model
    in
        setExecutionState queryId executionId model { executionState | mutationState = mutationState }


{-| Query is complete. N.B. this is called from APIs when a mutation error or some other error occurs.
    Otherwise this called automatically by this module when the query finishes.
-}
queryComplete : Model mutation mutationState completionTagger msg -> QueryId -> QueryResult -> ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) )
queryComplete model queryId result =
    let
        queryState =
            getQueryState queryId model

        currentExecutionId =
            getCurrentExecutionId queryId model

        maybeError =
            result |??> always Nothing ??= Just
    in
        setQueryState queryId model { queryState | maybeError = maybeError, currentExecutionId = Nothing } ! [ EventEmitter.trigger currentExecutionId ]


{-| Query Result tagger
-}
type alias ApiQueryResultTagger payload error msg =
    Result error payload -> msg


{-| Query Slate DB with persistent queries support
-}
fetch :
    Query (Msg mutation completionTagger)
    -> (() -> ApiExecutionState mutationState)
    -> Config mutation mutationState completionTagger msg
    -> DbConnectionInfo
    -> Model mutation mutationState completionTagger msg
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> List EntityId
    -> Maybe (List PropertyName)
    -> Bool
    -> String
    -> ( QueryId, Model mutation mutationState completionTagger msg, Cmd msg )
fetch query constructExecutionState config dbConnectionInfo model queryCompletionTagger completionTagger ids maybeProperties persist apiCallName =
    let
        listenerTagger =
            queryCompletionTagger (completionTagger queryId)

        ( queryId, ( model2, queryCmd ) ) =
            let
                initialFetch : Bool -> ( QueryId, ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ) )
                initialFetch =
                    executeQuery config dbConnectionInfo model query ids
            in
                Maybe.or (Dict.get apiCallName model.persitentQueryIds) model.refetchQueryId
                    |?> (\queryId ->
                            ( queryId
                            , getMaybeCurrentExecutionId queryId model
                                |?> always ( model, Cmd.none )
                                ?= refresh config dbConnectionInfo model queryId
                            )
                        )
                    ?= initialFetch persist

        ( executionId, model3, newQueryState ) =
            getMaybeCurrentExecutionId queryId model2
                |?> (\executionId ->
                        let
                            queryState =
                                getQueryState queryId model2

                            executionState =
                                getExecutionState queryId executionId model2
                        in
                            ( executionId, model2, { queryState | executionStates = Dict.insert executionId { executionState | executionCount = executionState.executionCount + 1 } queryState.executionStates } )
                    )
                ?= let
                    maybeQueryState =
                        getMaybeQueryState queryId model2

                    ( model5, executionId ) =
                        getNextExecutionId model2

                    insertExectionState =
                        Dict.insert executionId (constructExecutionState ())

                    newQueryState =
                        maybeQueryState
                            |?> (\queryState ->
                                    { queryState | executionStates = insertExectionState queryState.executionStates, currentExecutionId = Just executionId }
                                )
                            ?= { persist = persist
                               , currentExecutionId = Just executionId
                               , executionStates = insertExectionState Dict.empty
                               , maybeError = Nothing
                               }
                   in
                    ( executionId
                    , model5
                    , newQueryState
                    )

        model4 =
            setQueryState queryId (persist ? ( { model3 | persitentQueryIds = Dict.insert apiCallName queryId model3.persitentQueryIds }, model3 )) newQueryState
    in
        ( queryId, model4, Cmd.map config.routeToMeTagger <| Cmd.batch [ queryCmd, EventEmitter.listenOnce (QueryCompleteEvent listenerTagger queryId maybeProperties) executionId ] )


watchErrorCrash : List String -> x
watchErrorCrash errors =
    Debug.crash "Program bug: Watch Errors:" <| String.join "\n" errors


{-| Fetch and Watch
-}
fetchAndWatch :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> ApiName
    -> FetchFunction mutation mutationState completionTagger msg
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> msg
    -> FetchAndWatchFunctionReturn mutation mutationState completionTagger msg
fetchAndWatch config model apiName apiFetchFunction queryCompletionTagger completionTagger identityMsg =
    apiFetchFunction model
        |> (\( query, ( queryId, model, fetchCmd ) ) ->
                getQueryEventTypes query
                    |> (\queryEventTypes ->
                            dbWatch config model queryEventTypes queryId apiName apiFetchFunction queryCompletionTagger completionTagger identityMsg
                                |??> (\( model, watchCmd ) -> ( query, ( queryId, model, Cmd.batch [ fetchCmd, watchCmd ] ) ))
                                ??= (\error -> ( query, ( queryId, model, sendParentUpdateMsg <| config.onError (queryCompletionTagger (completionTagger queryId)) ( NonFatalError, error +-+ "(QueryId:" +-+ queryId +-+ ")" ) ) ))
                       )
           )


{-| Unwatch queryId
-}
unwatch :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> List QueryId
    -> ( Model mutation mutationState completionTagger msg, Cmd msg )
unwatch config model queryIds =
    queryIds
        |> List.foldl
            (\queryId ( model, cmds ) ->
                (isWatched model queryId)
                    ? ( { model | watches = Dict.remove queryId model.watches }
                            |> (\model ->
                                    getMaybeCurrentExecutionId queryId model
                                        |?> always model
                                        ?= disposeQuery model queryId
                               )
                            |> (\model ->
                                    DbWatcher.unsubscribe (dbWatcherConfig config) model.dbWatcherModel queryId
                                        |??> (\( dbWatcherModel, dbWatcherCmd ) -> ( model, Cmd.map config.routeToMeTagger dbWatcherCmd ))
                                        ??= watchErrorCrash
                               )
                      , ( model, Cmd.none )
                      )
                    |> (\( model, cmd ) -> ( model, cmd :: cmds ))
            )
            ( model, [] )
        |> (\( model, cmds ) -> ( model, Cmd.batch cmds ))


{-| Unwatch all watches for specified Api
-}
unwatchAll :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> ApiName
    -> ( Model mutation mutationState completionTagger msg, Cmd msg )
unwatchAll config model apiName =
    model.watches
        |> Dict.filter (\_ watches -> watches |> List.map (\( _, apiName, _, _ ) -> apiName) |> List.member apiName)
        |> Dict.keys
        |> unwatch config model
