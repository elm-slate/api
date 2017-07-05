module Slate.Api.Query
    exposing
        ( Config
        , WrappedConfig
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
        , PersistedState
        , unwrapConfig
        , init
        , start
        , stop
        , persistedStateEncoder
        , persistedStateDecoder
        , load
        , save
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

@docs Config, WrappedConfig, Model, Msg, QueryError, QueryResult, ExecutionId, ApiExecutionState, ApiQueryState, ApiQueryResultTagger, FetchFunctionReturn, FetchAndWatchFunctionReturn, PersistedState, unwrapConfig, init, start, stop, persistedStateEncoder, persistedStateDecoder, load, save, update, subscriptions, queryComplete, getQueryState, setQueryState, getCurrentExecutionId, getCurrentExecutionState, getCurrentMutationState, setCurrentMutationState, fetch, fetchAndWatch, unwatch, unwatchAll
-}

import Tuple exposing (..)
import Time exposing (Time)
import Task
import Process
import Dict as Dict exposing (Dict)
import Json.Decode as JD exposing (field)
import Json.Encode as JE
import Utils.Json as Json exposing ((///), (<||))
import Maybe.Extra as Maybe
import List.Extra as List
import Result.Extra as Result
import StringUtils exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.StateMachine exposing (..)
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


{-| Query Error.
-}
type alias QueryError =
    ( ErrorType, String )


{-| State per query
-}
type alias ApiQueryState mutationState =
    { persist : Bool
    , currentExecutionId : Maybe ExecutionId
    , executionStates : Dict ExecutionId (ApiExecutionState mutationState)
    , maybeError : Maybe QueryError
    }


{-| Module's config
-}
type alias Config mutation mutationState completionTagger msg =
    { ownerPath : String
    , connectionRetryMax : Int
    , routeToMeTagger : Msg mutation completionTagger -> msg
    , dbWatcherReconnectDelayInterval : Time
    , dbWatcherStopDelayInterval : Time
    , returnCmds : List (Cmd msg) -> msg
    , logTagger : LogTagger String msg
    , errorTagger : ErrorTagger String msg
    , onError : completionTagger -> QueryError -> msg
    , onComplete : ApiExecutionState mutationState -> completionTagger -> Maybe (List PropertyName) -> msg
    , onMutate : WrappedConfig mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> mutation -> ( ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ), List msg )
    , queryBatchSize : Int
    , debug : Bool
    }


{-| helper to unwrap config
-}
unwrapConfig : WrappedConfig mutation mutationState completionTagger msg -> Config mutation mutationState completionTagger msg
unwrapConfig wrappedConfig =
    case wrappedConfig of
        WrappedConfig config ->
            config


{-| Wrapped Config to avoid circular alias definition
-}
type WrappedConfig mutation mutationState completionTagger msg
    = WrappedConfig (Config mutation mutationState completionTagger msg)


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


type alias Watch mutation mutationState completionTagger msg =
    ( msg, FetchOperation mutation mutationState completionTagger msg, completionTagger )


type RunningState
    = InitState
    | LoadState
    | StartState
    | SaveState
    | StopState


stateMachine : StateMachine RunningState
stateMachine =
    validateStateMachine
        { makeComparable = toString
        , initialStates = [ InitState ]
        , edges =
            [ ( InitState, StartState )
            , ( InitState, LoadState )
            , ( LoadState, StartState )
            , ( StartState, StopState )
            , ( StartState, SaveState )
            , ( SaveState, SaveState )
            , ( SaveState, StopState )
            ]
        , terminalStates = [ StopState ]
        }


type alias ApiCallName =
    String


{-| Modules model
-}
type alias Model mutation mutationState completionTagger msg =
    { runningState : RunningState
    , nextExecutionId : Int
    , queryStates : Dict QueryId (ApiQueryState mutationState)
    , engineModel : Engine.Model (Msg mutation completionTagger)
    , dbWatcherModel : DbWatcher.Model
    , persistentQueryIds : Dict ApiCallName QueryId
    , watches : Dict QueryId (List (Watch mutation mutationState completionTagger msg))
    , refetchQueryId : Maybe QueryId
    , dbConnectionInfo : Maybe DbConnectionInfo
    , persistedEngineQueries : Maybe (Dict ApiCallName String)
    }


engineConfig : Config mutation mutationState completionTagger msg -> Engine.Config (Msg mutation completionTagger)
engineConfig config =
    { debug = config.debug
    , connectionRetryMax = config.connectionRetryMax
    , logTagger = EngineLog
    , errorTagger = EngineError
    , eventProcessingErrorTagger = EventProcessingError
    , completionTagger = EventProcessingComplete
    , routeToMeTagger = EngineMsg
    , queryBatchSize = config.queryBatchSize
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


enforceState : RunningState -> Model mutation mutationState completionTagger msg -> Result String (Model mutation mutationState completionTagger msg)
enforceState newState model =
    validateTransition stateMachine (Just model.runningState) newState
        |??> (\_ -> { model | runningState = newState })


initModel :
    Config mutation mutationState completionTagger msg
    -> ( Model mutation mutationState completionTagger msg, List (Cmd (Msg mutation completionTagger)) )
initModel config =
    let
        ( engineModel, engineCmd ) =
            Engine.init (engineConfig config)

        ( dbWatcherModel, dbWatcherCmd ) =
            DbWatcher.init (dbWatcherConfig config)
    in
        ( { runningState = InitState
          , nextExecutionId = 0
          , queryStates = Dict.empty
          , engineModel = engineModel
          , dbWatcherModel = dbWatcherModel
          , persistentQueryIds = Dict.empty
          , watches = Dict.empty
          , refetchQueryId = Nothing
          , dbConnectionInfo = Nothing
          , persistedEngineQueries = Nothing
          }
        , [ engineCmd, dbWatcherCmd ]
        )


{-| Initialize command processor
-}
init : Config mutation mutationState completionTagger msg -> ( Model mutation mutationState completionTagger msg, Cmd msg )
init config =
    let
        ( model, cmds ) =
            initModel config
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
    enforceState StartState model
        |??>
            (\model ->
                DbWatcher.start (dbWatcherConfig config) model.dbWatcherModel [ dbConnectionInfo ]
                    |> (\( dbWatcherModel, dbWatcherCmd ) -> Ok <| { model | dbConnectionInfo = Just dbConnectionInfo, dbWatcherModel = dbWatcherModel } ! [ Cmd.map config.routeToMeTagger dbWatcherCmd ])
            )
        ??= Err


{-| Stop API
-}
stop :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> Result String ( Model mutation mutationState completionTagger msg, Cmd msg )
stop config model =
    enforceState StopState model
        |??>
            (\model ->
                unwatchAll config model
                    |> (\( model, unwatchAllCmd ) ->
                            DbWatcher.stop (dbWatcherConfig config) model.dbWatcherModel
                                |> (\( dbWatcherModel, dbWatcherCmd ) -> Ok <| { model | dbConnectionInfo = Nothing, dbWatcherModel = dbWatcherModel } ! [ Cmd.map config.routeToMeTagger dbWatcherCmd, unwatchAllCmd ])
                       )
            )
        ??= Err


{-| persisted state
-}
type alias PersistedState mutationState =
    { engineQueries : Dict ApiCallName String
    , apiQueryStates : Dict QueryId (ApiQueryState mutationState)
    }


executionStateEncoder : (mutationState -> JE.Value) -> ApiExecutionState mutationState -> JE.Value
executionStateEncoder mutationStateEncoder executionState =
    JE.object
        [ ( "executionCount", JE.int executionState.executionCount )
        , ( "mutationState", mutationStateEncoder executionState.mutationState )
        ]


queryStateEncoder : (mutationState -> JE.Value) -> ApiQueryState mutationState -> JE.Value
queryStateEncoder mutationStateEncoder queryState =
    JE.object
        [ ( "persist", JE.bool queryState.persist )
        , ( "currentExecutionId", Json.encMaybe JE.string queryState.currentExecutionId )
        , ( "executionStates", Json.encDict JE.string (executionStateEncoder mutationStateEncoder) queryState.executionStates )
        , ( "maybeError"
          , Json.encMaybe
                (\maybeError ->
                    JE.object
                        [ ( "first", errorTypeEncoder (first maybeError) )
                        , ( "second", JE.string (second maybeError) )
                        ]
                )
                queryState.maybeError
          )
        ]


{-| PersistedState encoder
-}
persistedStateEncoder : (mutationState -> JE.Value) -> PersistedState mutationState -> JE.Value
persistedStateEncoder mutationStateEncoder persistedState =
    JE.object
        [ ( "engineQueries", Json.encDict JE.string JE.string persistedState.engineQueries )
        , ( "apiQueryStates", Json.encDict JE.int (queryStateEncoder mutationStateEncoder) persistedState.apiQueryStates )
        ]


executionStateDecoder : JD.Decoder mutationState -> JD.Decoder (ApiExecutionState mutationState)
executionStateDecoder mutationStateDecoder =
    (JD.succeed <| ApiExecutionState)
        <|| (field "executionCount" JD.int)
        <|| (field "mutationState" mutationStateDecoder)


queryStateDecoder : JD.Decoder mutationState -> JD.Decoder (ApiQueryState mutationState)
queryStateDecoder mutationStateDecoder =
    (JD.succeed ApiQueryState)
        <|| (field "persist" JD.bool)
        <|| (JD.maybe (field "currentExecutionId" JD.string))
        <|| (field "executionStates" <| Json.decDict JD.string (executionStateDecoder mutationStateDecoder))
        <|| (JD.maybe
                (field "maybeError" <|
                    (JD.succeed (,))
                        <|| (field "first" errorTypeDecoder)
                        <|| (field "second" JD.string)
                )
            )


{-| PersistedState decoder
-}
persistedStateDecoder : JD.Decoder mutationState -> JD.Decoder (PersistedState mutationState)
persistedStateDecoder mutationStateDecoder =
    (JD.succeed PersistedState)
        <|| (field "engineQueries" <| Json.decDict JD.string JD.string)
        <|| (field "apiQueryStates" <| Json.decDict JD.int (queryStateDecoder mutationStateDecoder))


{-| Load persisted state
-}
load :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> PersistedState mutationState
    -> Result String (Model mutation mutationState completionTagger msg)
load config model persistedState =
    enforceState LoadState model
        |??> (\model -> Ok { model | persistedEngineQueries = Just persistedState.engineQueries })
        ??= Err


{-| Save persisted state
-}
save :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> Result String ( Model mutation mutationState completionTagger msg, PersistedState mutationState )
save config model =
    enforceState SaveState model
        |??>
            (\model ->
                (model.persistentQueryIds
                    |> Dict.filter (\_ -> Engine.isGoodQueryState model.engineModel)
                    |> Dict.map (\_ -> Engine.exportQueryState model.engineModel)
                    |> (\dict ->
                            (dict |> Dict.filter (\_ -> Result.isErr))
                                |> (\dictErrors ->
                                        model.queryStates
                                            |> Dict.filter (\_ -> .persist)
                                            |> Dict.map (\_ queryState -> { queryState | executionStates = Dict.map (\_ state -> { state | executionCount = 0 }) queryState.executionStates, currentExecutionId = Nothing, maybeError = Nothing })
                                            |> (\apiQueryStates ->
                                                    (Dict.size dictErrors == 0)
                                                        ? ( Ok <| ( model, { engineQueries = Dict.map (\_ -> (flip (??=)) (always "")) dict, apiQueryStates = apiQueryStates } )
                                                          , Err <| ((List.map ((flip (??=)) (always "")) <| Dict.values dictErrors) |> String.join "\n")
                                                          )
                                               )
                                   )
                       )
                )
            )
        ??= Err


isWatched : Model mutation mutationState completionTagger msg -> QueryId -> Bool
isWatched model queryId =
    Dict.member queryId model.watches


disposeQuery : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> Model mutation mutationState completionTagger msg
disposeQuery config model queryId =
    let
        queryState =
            getQueryState config model queryId
    in
        (queryState.persist || isWatched model queryId)
            ? ( model, { model | engineModel = Engine.disposeQuery model.engineModel queryId, queryStates = Dict.remove queryId model.queryStates } )


countDownExecution : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ExecutionId -> Model mutation mutationState completionTagger msg
countDownExecution config model queryId executionId =
    let
        executionState =
            getExecutionState config model queryId executionId

        queryState =
            getQueryState config model queryId
    in
        queryState.persist
            ? ( setExecutionState config model queryId executionId { executionState | executionCount = executionState.executionCount - 1 }
                -- let
                --         executionCount =
                --             executionState.executionCount - 1
                --     in
                --      (executionCount == 0)
                --     ? ( setQueryState queryId model { queryState | executionStates = Dict.remove executionId queryState.executionStates }
                --       , setExecutionState config model queryId executionId { executionState | executionCount = executionCount }
                --       )
              , disposeQuery config model queryId
              )


getMaybeQueryState : QueryId -> Model mutation mutationState completionTagger msg -> Maybe (ApiQueryState mutationState)
getMaybeQueryState queryId model =
    Dict.get queryId model.queryStates


getExecutionState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ExecutionId -> ApiExecutionState mutationState
getExecutionState config model queryId executionId =
    let
        queryState =
            getQueryState config model queryId
    in
        Dict.get executionId queryState.executionStates ?!= (\_ -> Debug.crash ("Cannot get executionState for queryId:" +-+ "(" ++ config.ownerPath ++ ")" +-+ queryId +-+ "executionId:" +-+ executionId))


getNextExecutionId : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> ( Model mutation mutationState completionTagger msg, ExecutionId )
getNextExecutionId config model =
    let
        nextExecutionId =
            model.nextExecutionId + 1

        executionId =
            config.ownerPath ++ (toString nextExecutionId)
    in
        ( { model | nextExecutionId = nextExecutionId }, executionId )


getMaybeCurrentExecutionId : QueryId -> Model mutation mutationState completionTagger msg -> Maybe ExecutionId
getMaybeCurrentExecutionId queryId model =
    Dict.get queryId model.queryStates |?> .currentExecutionId ?= Nothing


setExecutionState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ExecutionId -> ApiExecutionState mutationState -> Model mutation mutationState completionTagger msg
setExecutionState config model queryId executionId executionState =
    let
        queryState =
            getQueryState config model queryId
    in
        setQueryState queryId model { queryState | executionStates = Dict.insert executionId executionState queryState.executionStates }


executeQuery : Config mutation mutationState completionTagger msg -> DbConnectionInfo -> Model mutation mutationState completionTagger msg -> Query (Msg mutation completionTagger) -> List EntityId -> ( QueryId, ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) ) )
executeQuery config dbConnectionInfo model query ids =
    let
        result =
            Engine.executeQuery (engineConfig config) dbConnectionInfo model.engineModel Nothing query ids
    in
        result
            |??>
                (\( engineModel, cmd, queryId ) ->
                    ( queryId, { model | engineModel = engineModel } ! [ cmd ] )
                )
            ??= (\errors -> Debug.crash <| "Query error:" +-+ "(" ++ config.ownerPath ++ ")" +-+ (String.join "\n" errors))


refresh : Config mutation mutationState completionTagger msg -> DbConnectionInfo -> Model mutation mutationState completionTagger msg -> QueryId -> ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) )
refresh config dbConnectionInfo model queryId =
    Engine.refreshQuery (engineConfig config) dbConnectionInfo model.engineModel queryId
        |??>
            (\( engineModel, cmd ) ->
                let
                    queryState =
                        getQueryState config model queryId
                in
                    { model | engineModel = engineModel } ! [ cmd ]
            )
        ??= (\error -> Debug.crash <| "Refresh error:" +-+ "(" ++ config.ownerPath ++ ")" +-+ error)


findWatch : QueryId -> msg -> Model mutation mutationState completionTagger msg -> Maybe (Watch mutation mutationState completionTagger msg)
findWatch queryId identityMsg model =
    (Dict.get queryId model.watches)
        |?> List.find (\( msg, _, _ ) -> msg == identityMsg)
        ?= Nothing


getDbConnectionInfo : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> DbConnectionInfo
getDbConnectionInfo config model =
    model.dbConnectionInfo ?!= (\_ -> Debug.crash ("Query not started" +-+ "(" ++ config.ownerPath ++ ")"))


dbWatch :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> List EntityEventTypes
    -> QueryId
    -> FetchFunction mutation mutationState completionTagger msg
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> msg
    -> Result (List String) ( Model mutation mutationState completionTagger msg, Cmd msg )
dbWatch config model queryEventTypes queryId apiFetchFunction queryCompletionTagger completionTagger identityMsg =
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
            ?= (DbWatcher.subscribe (dbWatcherConfig config) (getDbConnectionInfo config model) model.dbWatcherModel queryEventTypes queryId
                    |??>
                        (\( dbWatcherModel, dbWatcherCmd ) ->
                            { model
                                | watches =
                                    (Dict.insert queryId
                                        (( identityMsg, Fetch interceptedApiFetchFunction, queryCompletionTagger (completionTagger queryId) ) :: (Dict.get queryId model.watches ?= []))
                                        model.watches
                                    )
                                , dbWatcherModel = dbWatcherModel
                            }
                                ! [ Cmd.map config.routeToMeTagger dbWatcherCmd ]
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
                ( model ! [], [ logMsg ("Engine:" +-+ "(" ++ config.ownerPath ++ ")" +-+ ( queryId, message )) ] )

            EngineError ( errorType, ( queryId, error ) ) ->
                let
                    queryState =
                        getQueryState config model queryId
                in
                    (queryComplete config model queryId <| Err ( errorType, "Engine Error:" +-+ "(" ++ config.ownerPath ++ ")" +-+ error ))
                        |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            DbWatcherMsg msg ->
                updateDbWatcher msg model

            DbWatcherLog ( logLevel, details ) ->
                ( model ! [], [ config.logTagger <| ( logLevel, "DbWatcher:" +-+ "(" ++ config.ownerPath ++ ")" +-+ details ) ] )

            DbWatcherError ( errorType, details ) ->
                case errorType of
                    FatalError ->
                        Debug.crash ("Fatal DbWatcherError:" +-+ "(" ++ config.ownerPath ++ ")" +-+ details)

                    _ ->
                        ( model ! [], [ config.errorTagger ( errorType, "(" ++ config.ownerPath ++ ")" +-+ details ) ] )

            DbWatcherRefreshRequired queryIds ->
                let
                    ( newModel, cmds ) =
                        queryIds
                            |> List.filterMap
                                (\queryId ->
                                    Dict.get queryId model.watches
                                        |?> List.map (\( _, fetchOperation, _ ) -> fetchOperation)
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
                Debug.crash <| toString ( queryId, "UnspecifiedMutationInQuery:" +-+ "(" ++ config.ownerPath ++ ")" +-+ eventRecord )

            EventError eventRecord ( queryId, error ) ->
                let
                    queryState =
                        getQueryState config model queryId
                in
                    (queryComplete config model queryId <| Err ( NonFatalError, "EventError:" +-+ "(" ++ config.ownerPath ++ ")" +-+ ( queryId, error ) +-+ "for eventRecord:" +-+ eventRecord ))
                        |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            EventProcessingError ( eventStr, error ) ->
                Debug.crash <| "Event Processing Error:" +-+ "(" ++ config.ownerPath ++ ")" +-+ error +-+ "\nEvent:" +-+ eventStr

            Mutate queryId mutation ->
                config.onMutate (WrappedConfig config) model queryId mutation
                    |> (\( ( model, cmd ), msgs ) -> ( model ! [ cmd ], msgs ))

            EventProcessingComplete queryId ->
                (queryComplete config model queryId <| Ok ())
                    |> (\( model, cmd ) -> ( model ! [ cmd ], [] ))

            QueryCompleteEvent completionTagger queryId maybeProperties executionId ->
                let
                    queryState =
                        getQueryState config model queryId

                    executionState =
                        getExecutionState config model queryId executionId

                    msg =
                        queryState.maybeError
                            |?> config.onError completionTagger
                            ?= config.onComplete executionState completionTagger maybeProperties
                in
                    ( countDownExecution config model queryId executionId ! [], [ msg ] )


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
getQueryState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ApiQueryState mutationState
getQueryState config model queryId =
    getMaybeQueryState queryId model ?!= (\_ -> Debug.crash ("Cannot find queryState for queryId:" +-+ "(" ++ config.ownerPath ++ ")" +-+ queryId))


{-| Set query state for speicified queryId
-}
setQueryState : QueryId -> Model mutation mutationState completionTagger msg -> ApiQueryState mutationState -> Model mutation mutationState completionTagger msg
setQueryState queryId model queryState =
    { model | queryStates = Dict.insert queryId queryState model.queryStates }


{-| Get current execution id for specified queryId
-}
getCurrentExecutionId : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ExecutionId
getCurrentExecutionId config model queryId =
    getMaybeCurrentExecutionId queryId model ?!= (\_ -> Debug.crash ("Cannot get currentExecutionId for queryId:" +-+ "(" ++ config.ownerPath ++ ")" +-+ queryId))


{-| Get current execution state for specified queryId
-}
getCurrentExecutionState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> ApiExecutionState mutationState
getCurrentExecutionState config model queryId =
    let
        executionId =
            getCurrentExecutionId config model queryId
    in
        getExecutionState config model queryId executionId


{-| Get current Entity MutationState for specified queryId
-}
getCurrentMutationState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> mutationState
getCurrentMutationState config model queryId =
    let
        executionId =
            getCurrentExecutionId config model queryId
    in
        getExecutionState config model queryId executionId
            |> .mutationState


{-| Set current Entity MutationState for specified queryId
-}
setCurrentMutationState : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> mutationState -> Model mutation mutationState completionTagger msg
setCurrentMutationState config model queryId mutationState =
    let
        executionId =
            getCurrentExecutionId config model queryId

        executionState =
            getExecutionState config model queryId executionId
    in
        setExecutionState config model queryId executionId { executionState | mutationState = mutationState }


{-| Query is complete. N.B. this is called from APIs when a mutation error or some other error occurs.
    Otherwise this called automatically by this module when the query finishes.
-}
queryComplete : Config mutation mutationState completionTagger msg -> Model mutation mutationState completionTagger msg -> QueryId -> QueryResult -> ( Model mutation mutationState completionTagger msg, Cmd (Msg mutation completionTagger) )
queryComplete config model queryId result =
    getCurrentExecutionId config model queryId
        |> (\currentExecutionId ->
                let
                    queryState =
                        getQueryState config model queryId

                    maybeError =
                        result |??> always Nothing ??= Just
                in
                    setQueryState queryId model { queryState | maybeError = maybeError, currentExecutionId = Nothing } ! [ EventEmitter.trigger currentExecutionId ]
           )


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
    (ids /= [] && persist)
        ?! ( \_ -> Debug.crash ("Cannot persist query if ids specified" +-+ "(" ++ config.ownerPath ++ ")"), always "" )
        |> (always
                ((model.persistedEngineQueries
                    |?> (\persistedEngineQueries ->
                            (Dict.get apiCallName persistedEngineQueries)
                                |?> (\json ->
                                        Dict.remove apiCallName persistedEngineQueries
                                            |> (\persistedEngineQueries ->
                                                    Engine.importQueryState query model.engineModel json
                                                        |??>
                                                            (\( queryId, engineModel ) ->
                                                                getNextExecutionId config model
                                                                    |> (\( model, executionId ) ->
                                                                            { persist = persist
                                                                            , currentExecutionId = Nothing
                                                                            , executionStates = Dict.empty
                                                                            , maybeError = Nothing
                                                                            }
                                                                                |> setQueryState queryId (persist ? ( { model | persistentQueryIds = Dict.insert apiCallName queryId model.persistentQueryIds }, model ))
                                                                                |> (\model -> ( { model | persistedEngineQueries = Just persistedEngineQueries, engineModel = engineModel }, Just queryId ))
                                                                       )
                                                            )
                                                        ??= (\error ->
                                                                config.debug
                                                                    ? ( Debug.log ("*** DEBUG:SlateApi importQueryState for" +-+ "(" ++ config.ownerPath ++ ")" +-+ apiCallName) error, "" )
                                                                    |> always ( model, Nothing )
                                                            )
                                               )
                                    )
                                ?= ( model, Nothing )
                        )
                    ?= ( model, Nothing )
                 )
                    |> (\( model, maybePersistedQueryId ) ->
                            (Maybe.or maybePersistedQueryId <| Maybe.or (Dict.get apiCallName model.persistentQueryIds) model.refetchQueryId)
                                |?> (\queryId ->
                                        ( queryId
                                        , getMaybeCurrentExecutionId queryId model
                                            |?> always ( model, Cmd.none )
                                            ?= refresh config dbConnectionInfo model queryId
                                        )
                                    )
                                ?= executeQuery config dbConnectionInfo model query ids
                                |> (\( queryId, ( model, queryCmd ) ) ->
                                        (getMaybeCurrentExecutionId queryId model
                                            |?> (\executionId ->
                                                    ( getQueryState config model queryId, getExecutionState config model queryId executionId )
                                                        |> (\( queryState, executionState ) ->
                                                                ( executionId
                                                                , model
                                                                , { queryState | executionStates = Dict.insert executionId { executionState | executionCount = executionState.executionCount + 1 } queryState.executionStates }
                                                                )
                                                           )
                                                )
                                            ?= (getNextExecutionId config model
                                                    |> (\( model, executionId ) ->
                                                            Dict.insert executionId (constructExecutionState ())
                                                                |> (\executionStateToInsert ->
                                                                        ( executionId
                                                                        , model
                                                                        , getMaybeQueryState queryId model
                                                                            |?> (\queryState -> { queryState | executionStates = executionStateToInsert queryState.executionStates, currentExecutionId = Just executionId })
                                                                            ?= { persist = persist
                                                                               , currentExecutionId = Just executionId
                                                                               , executionStates = executionStateToInsert Dict.empty
                                                                               , maybeError = Nothing
                                                                               }
                                                                        )
                                                                   )
                                                       )
                                               )
                                        )
                                            |> (\( executionId, model, queryState ) ->
                                                    setQueryState queryId (persist ? ( { model | persistentQueryIds = Dict.insert apiCallName queryId model.persistentQueryIds }, model )) queryState
                                                        |> (\model ->
                                                                ( queryId, model, Cmd.map config.routeToMeTagger <| Cmd.batch [ queryCmd, EventEmitter.listenOnce (QueryCompleteEvent (queryCompletionTagger (completionTagger queryId)) queryId maybeProperties) executionId ] )
                                                           )
                                               )
                                   )
                       )
                )
           )


watchErrorCrash : Config mutation mutationState completionTagger msg -> List String -> x
watchErrorCrash config errors =
    Debug.crash ("Program bug: Watch Errors:" +-+ "(" ++ config.ownerPath ++ ")" +-+ (String.join "\n" errors))


{-| Fetch and Watch
-}
fetchAndWatch :
    Config mutation mutationState completionTagger msg
    -> Model mutation mutationState completionTagger msg
    -> FetchFunction mutation mutationState completionTagger msg
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> msg
    -> FetchAndWatchFunctionReturn mutation mutationState completionTagger msg
fetchAndWatch config model apiFetchFunction queryCompletionTagger completionTagger identityMsg =
    apiFetchFunction model
        |> (\( query, ( queryId, model, fetchCmd ) ) ->
                getQueryEventTypes query
                    |> (\queryEventTypes ->
                            dbWatch config model queryEventTypes queryId apiFetchFunction queryCompletionTagger completionTagger identityMsg
                                |??> (\( model, watchCmd ) -> ( query, ( queryId, model, Cmd.batch [ fetchCmd, watchCmd ] ) ))
                                ??= (\error -> ( query, ( queryId, model, sendParentUpdateMsg <| config.onError (queryCompletionTagger (completionTagger queryId)) ( NonFatalError, error +-+ "(QueryId:" +-+ queryId ++ ")" ) ) ))
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
                                        ?= disposeQuery config model queryId
                               )
                            |> (\model ->
                                    DbWatcher.unsubscribe (dbWatcherConfig config) model.dbWatcherModel queryId
                                        |??> (\( dbWatcherModel, dbWatcherCmd ) -> ( model, Cmd.map config.routeToMeTagger dbWatcherCmd ))
                                        ??= watchErrorCrash config
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
    -> ( Model mutation mutationState completionTagger msg, Cmd msg )
unwatchAll config model =
    model.watches
        |> Dict.keys
        |> unwatch config model
