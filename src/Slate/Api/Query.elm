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
        , init
        , update
        , queryComplete
        , getQueryState
        , setQueryState
        , getCurrentExecutionId
        , getCurrentExecutionState
        , getCurrentFragments
        , setCurrentFragments
        , query
        )

{-|
    Common Query code for creating Query APIs.

@docs Config , Model , Msg , QueryError , QueryResult , ExecutionId , ApiExecutionState , ApiQueryState , ApiQueryResultTagger , init , update , queryComplete , getQueryState , setQueryState , getCurrentExecutionId , getCurrentExecutionState , getCurrentFragments , setCurrentFragments , query
-}

import Dict as Dict exposing (Dict)
import StringUtils exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Engine.Query as Query exposing (..)
import Slate.Engine.Engine as Engine exposing (..)
import ParentChildUpdate exposing (..)
import Slate.Common.Taggers exposing (..)
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
type alias ApiExecutionState fragments =
    { executionCount : Int
    , fragments : fragments
    }


type alias ExecutionStates fragments =
    Dict ExecutionId (ApiExecutionState fragments)


{-| Query Error.
-}
type alias QueryError =
    ( ErrorType, String )


{-| State per query
-}
type alias ApiQueryState fragments =
    { persist : Bool
    , currentExecutionId : Maybe ExecutionId
    , executionStates : ExecutionStates fragments
    , maybeError : Maybe QueryError
    }


{-| Module's config
-}
type alias Config mutation fragments completionTagger msg =
    { routeToMeTagger : Msg mutation fragments completionTagger -> msg
    , logTagger : LogTagger String msg
    , onError : completionTagger -> QueryError -> msg
    , onComplete : ApiExecutionState fragments -> completionTagger -> msg
    , onMutate : mutation -> Model mutation fragments completionTagger -> ( ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) ), List msg )
    }


{-| Query Result
-}
type alias QueryResult =
    Result QueryError ()


{-| Modules model
-}
type alias Model mutation fragments completionTagger =
    { fullyQualifiedModuleName : String
    , nextExecutionId : Int
    , queryStates : Dict QueryId (ApiQueryState fragments)
    , engineModel : Engine.Model (Msg mutation fragments completionTagger)
    , persitentQueryIds : Dict String QueryId
    }


initModel : String -> ( Model mutation fragments completionTagger, List (Cmd (Msg mutation fragments completionTagger)) )
initModel fullyQualifiedModuleName =
    let
        ( engineModel, engineCmd ) =
            Engine.init engineConfig
    in
        ( { fullyQualifiedModuleName = fullyQualifiedModuleName
          , nextExecutionId = 0
          , queryStates = Dict.empty
          , engineModel = engineModel
          , persitentQueryIds = Dict.empty
          }
        , [ engineCmd ]
        )


{-| Initialize command processor
-}
init : Config mutation fragments completionTagger msg -> String -> ( Model mutation fragments completionTagger, Cmd msg )
init config fullyQualifiedModuleName =
    let
        ( model, cmds ) =
            initModel fullyQualifiedModuleName
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


engineConfig : Engine.Config (Msg mutation fragments completionTagger)
engineConfig =
    { debug = False
    , logTagger = EngineLog
    , errorTagger = EngineError
    , eventProcessingErrorTagger = EventProcessingError
    , completionTagger = EventProcessingComplete
    , routeToMeTagger = EngineMsg
    , queryBatchSize = 2000
    }


endQuery : Model mutation fragments completionTagger -> QueryId -> Model mutation fragments completionTagger
endQuery model queryId =
    let
        queryState =
            getQueryState queryId model
    in
        queryState.persist ? ( model, { model | engineModel = disposeQuery model.engineModel queryId, queryStates = Dict.remove queryId model.queryStates } )


countDownExecution : Model mutation fragments completionTagger -> QueryId -> ExecutionId -> Model mutation fragments completionTagger
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
              , endQuery model queryId
              )


getMaybeQueryState : QueryId -> Model mutation fragments completionTagger -> Maybe (ApiQueryState fragments)
getMaybeQueryState queryId model =
    Dict.get queryId model.queryStates


getExecutionState : QueryId -> ExecutionId -> Model mutation fragments completionTagger -> ApiExecutionState fragments
getExecutionState queryId executionId model =
    let
        queryState =
            getQueryState queryId model
    in
        Dict.get executionId queryState.executionStates ?!= (\_ -> Debug.crash ("Cannot get executionState for queryId:" +-+ queryId +-+ "executionId:" +-+ executionId))


getNextExecutionId : Model mutation fragments completionTagger -> ( Model mutation fragments completionTagger, ExecutionId )
getNextExecutionId model =
    let
        nextExecutionId =
            model.nextExecutionId + 1

        executionId =
            model.fullyQualifiedModuleName ++ (toString nextExecutionId)
    in
        ( { model | nextExecutionId = nextExecutionId }, executionId )


getMaybeCurrentExecutionId : QueryId -> Model mutation fragments completionTagger -> Maybe ExecutionId
getMaybeCurrentExecutionId queryId model =
    Dict.get queryId model.queryStates |?> .currentExecutionId ?= Nothing


setExecutionState : QueryId -> ExecutionId -> Model mutation fragments completionTagger -> ApiExecutionState fragments -> Model mutation fragments completionTagger
setExecutionState queryId executionId model executionState =
    let
        queryState =
            getQueryState queryId model
    in
        setQueryState queryId model { queryState | executionStates = Dict.insert executionId executionState queryState.executionStates }


executeQuery : DbConnectionInfo -> Model mutation fragments completionTagger -> Query (Msg mutation fragments completionTagger) -> List EntityId -> Bool -> ( QueryId, ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) ) )
executeQuery dbConnectionInfo model query ids persist =
    let
        result =
            Engine.executeQuery engineConfig dbConnectionInfo model.engineModel Nothing query ids
    in
        result
            |??>
                (\( engineModel, cmd, queryId ) ->
                    ( queryId, { model | engineModel = engineModel } ! [ cmd ] )
                )
            ??= (\errors -> Debug.crash <| "Query error:" +-+ (String.join "\n" errors))


refresh : DbConnectionInfo -> Model mutation fragments completionTagger -> QueryId -> ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) )
refresh dbConnectionInfo model queryId =
    Engine.refreshQuery engineConfig dbConnectionInfo model.engineModel queryId
        |??>
            (\( engineModel, cmd ) ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    { model | engineModel = engineModel } ! [ cmd ]
            )
        ??= (\error -> Debug.crash <| "Refresh error:" +-+ error)



-- API


{-| Module's Msgs
-}
type Msg mutation fragments completionTagger
    = EngineMsg Engine.Msg
    | EngineLog ( LogLevel, ( QueryId, String ) )
    | EngineError ( ErrorType, ( QueryId, String ) )
    | Mutate mutation
    | UnspecifiedMutationInQuery QueryId EventRecord
    | EventError EventRecord ( QueryId, String )
    | EventProcessingError ( String, String )
    | EventProcessingComplete QueryId
    | QueryCompleteEvent completionTagger QueryId ExecutionId


{-| Update.
-}
update : Config mutation fragments completionTagger msg -> Msg mutation fragments completionTagger -> Model mutation fragments completionTagger -> ( ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) ), List msg )
update config msg model =
    let
        logMsg message =
            config.logTagger ( LogLevelInfo, message )

        updateEngine =
            updateChildParent (Engine.update engineConfig) (update config) .engineModel engineConfig.routeToMeTagger (\model engineModel -> { model | engineModel = engineModel })
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
                    ( queryComplete model queryId <| Err ( errorType, error ), [] )

            UnspecifiedMutationInQuery queryId eventRecord ->
                Debug.crash <| toString ( queryId, "UnspecifiedMutationInQuery:" +-+ eventRecord )

            EventError eventRecord ( queryId, error ) ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    ( queryComplete model queryId <| Err ( NonFatalError, "EventError:" +-+ ( queryId, error ) +-+ "for eventRecord:" +-+ eventRecord ), [] )

            EventProcessingError ( eventStr, error ) ->
                Debug.crash <| "Event Processing Error:" +-+ error +-+ "\nEvent:" +-+ eventStr

            Mutate mutation ->
                config.onMutate mutation model

            EventProcessingComplete queryId ->
                let
                    queryState =
                        getQueryState queryId model
                in
                    ( queryComplete model queryId <| Ok (), [] )

            QueryCompleteEvent completionTagger queryId executionId ->
                let
                    queryState =
                        getQueryState queryId model

                    executionState =
                        getExecutionState queryId executionId model

                    msg =
                        queryState.maybeError
                            |?> config.onError completionTagger
                            ?= config.onComplete executionState completionTagger
                in
                    ( countDownExecution model queryId executionId ! [], [ msg ] )


{-| Get query state for speicified queryId
-}
getQueryState : QueryId -> Model mutation fragments completionTagger -> ApiQueryState fragments
getQueryState queryId model =
    getMaybeQueryState queryId model ?!= (\_ -> Debug.crash ("Cannot find queryState for queryId:" +-+ queryId))


{-| Set query state for speicified queryId
-}
setQueryState : QueryId -> Model mutation fragments completionTagger -> ApiQueryState fragments -> Model mutation fragments completionTagger
setQueryState queryId model queryState =
    { model | queryStates = Dict.insert queryId queryState model.queryStates }


{-| Get current execution id for specified queryId
-}
getCurrentExecutionId : QueryId -> Model mutation fragments completionTagger -> ExecutionId
getCurrentExecutionId queryId model =
    getMaybeCurrentExecutionId queryId model ?!= (\_ -> Debug.crash ("Cannot get currentExecutionId for queryId:" +-+ queryId))


{-| Get current execution state for specified queryId
-}
getCurrentExecutionState : QueryId -> Model mutation fragments completionTagger -> ApiExecutionState fragments
getCurrentExecutionState queryId model =
    let
        executionId =
            getCurrentExecutionId queryId model
    in
        getExecutionState queryId executionId model


{-| Get current Entity Fragments for specified queryId
-}
getCurrentFragments : QueryId -> Model mutation fragments completionTagger -> fragments
getCurrentFragments queryId model =
    let
        executionId =
            getCurrentExecutionId queryId model
    in
        getExecutionState queryId executionId model
            |> .fragments


{-| Set current Entity Fragments for specified queryId
-}
setCurrentFragments : QueryId -> Model mutation fragments completionTagger -> fragments -> Model mutation fragments completionTagger
setCurrentFragments queryId model fragments =
    let
        executionId =
            getCurrentExecutionId queryId model

        executionState =
            getExecutionState queryId executionId model
    in
        setExecutionState queryId executionId model { executionState | fragments = fragments }


{-| Query is complete. N.B. this is called from APIs when a mutation error or some other error occurs.
    Otherwise this called automatically by this module when the query finishes.
-}
queryComplete : Model mutation fragments completionTagger -> QueryId -> QueryResult -> ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) )
queryComplete model queryId result =
    let
        queryState =
            getQueryState queryId model

        currentExecutionId =
            getCurrentExecutionId queryId model

        maybeError =
            case result of
                Ok () ->
                    Nothing

                Err errorInfo ->
                    Just errorInfo
    in
        setQueryState queryId model { queryState | maybeError = maybeError, currentExecutionId = Nothing } ! [ EventEmitter.trigger currentExecutionId ]


{-| Query Result tagger
-}
type alias ApiQueryResultTagger payload error msg =
    Result error payload -> msg


{-| Query Slate DB with persistent queries
-}
query :
    Query (Msg mutation fragments completionTagger)
    -> (() -> ApiExecutionState fragments)
    -> Config mutation fragments completionTagger msg
    -> DbConnectionInfo
    -> Model mutation fragments completionTagger
    -> (ApiQueryResultTagger payload error msg -> completionTagger)
    -> (QueryId -> ApiQueryResultTagger payload error msg)
    -> List EntityId
    -> Maybe (List PropertyName)
    -> Bool
    -> String
    -> ( QueryId, Model mutation fragments completionTagger, Cmd msg )
query fetchQuery constructExecutionState config dbConnectionInfo model queryCompletionTagger completionTagger ids properties persist apiCallName =
    let
        listenerTagger =
            queryCompletionTagger (completionTagger queryId)

        ( queryId, ( model2, queryCmd ) ) =
            let
                initialFetch : Bool -> ( QueryId, ( Model mutation fragments completionTagger, Cmd (Msg mutation fragments completionTagger) ) )
                initialFetch =
                    executeQuery dbConnectionInfo model fetchQuery ids
            in
                persist
                    ? ( Dict.get apiCallName model.persitentQueryIds
                            |?> (\queryId ->
                                    ( queryId
                                    , getMaybeCurrentExecutionId queryId model
                                        |?> always ( model, Cmd.none )
                                        ?= refresh dbConnectionInfo model queryId
                                    )
                                )
                            ?= initialFetch True
                      , initialFetch False
                      )

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
        ( queryId, model4, Cmd.map config.routeToMeTagger <| Cmd.batch [ queryCmd, EventEmitter.listenOnce (QueryCompleteEvent listenerTagger queryId) executionId ] )
