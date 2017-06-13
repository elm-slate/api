module Slate.Api.Command
    exposing
        ( Config
        , Model
        , Msg(..)
        , ApiResult(..)
        , ApiCreationTagger
        , ApiOperationTagger
        , ApiNonMutatingTagger
        , ApiCreationResultTagger
        , ApiOperationResultTagger
        , ApiNonMutatingResultTagger
        , CustomValidationErrorHandler
        , init
        , update
        , processCreationMutatingEvents
        , processOperationMutatingEvents
        , processNonMutatingEvents
        , createEntityEvents
        , createEntity
        , destroyEntityEvents
        , destroyEntity
        , addPropertyEvents
        , addProperty
        , removePropertyEvents
        , removeProperty
        )

{-|
    Common Command code for creating Command APIs.

@docs Config , Model , Msg , ApiResult , ApiCreationTagger , ApiOperationTagger , ApiNonMutatingTagger , ApiCreationResultTagger , ApiOperationResultTagger , ApiNonMutatingResultTagger , CustomValidationErrorHandler , init , update , processCreationMutatingEvents , processOperationMutatingEvents , processNonMutatingEvents , createEntityEvents , createEntity , destroyEntityEvents , destroyEntity , addPropertyEvents , addProperty , removePropertyEvents , removeProperty
-}

import Tuple exposing (..)
import Dict exposing (Dict)
import Uuid
import Random.Pcg as Random exposing (..)
import StringUtils exposing (..)
import ParentChildUpdate exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Event exposing (..)
import Slate.Command.Helper exposing (..)
import Slate.Command.Common.Command exposing (..)
import Slate.Command.Common.Validation exposing (..)
import Slate.Command.Processor as CommandProcessor exposing (..)
import Slate.Common.Taggers exposing (..)
import Utils.Error exposing (..)
import Utils.Log exposing (..)
import Utils.Ops exposing (..)
import DebugF exposing (..)


{-| Common config
-}
type alias Config customValidationError error msg =
    { routeToMeTagger : Msg customValidationError msg -> msg
    , logTagger : LogTagger String msg
    , customValidationErrorHandler : CustomValidationErrorHandler customValidationError error msg
    , errorMsgToError : ErrorType -> String -> error
    , valueInvalid : error
    , valuesInvalid : List PropertyName -> error
    }


type alias CommandDict error msg =
    Dict CommandId (CommandState error msg)


type alias CommandState error msg =
    { tagger : ApiResult error msg
    }


{-| Common Model
-}
type alias Model customValidationError error msg =
    { seed : Seed
    , commandProcessorModel : CommandProcessor.Model customValidationError (Msg customValidationError msg)
    , commands : CommandDict error msg
    }


{-| Api Result
-}
type ApiResult error msg
    = CreationResult EntityId (ApiCreationResultTagger error msg)
    | OperationResult (ApiOperationResultTagger error msg)
    | NonMutatingResult (ApiNonMutatingResultTagger error msg)


{-| Creation tagger result for creation Api calls (used by App).
-}
type alias ApiCreationResultTagger error msg =
    Result error EntityId -> msg


{-| Creation tagger result for operational Api calls (used by App).
-}
type alias ApiOperationResultTagger error msg =
    Result error () -> msg


{-| Creation tagger result for operational Api calls (used by App).
-}
type alias ApiNonMutatingResultTagger error msg =
    Result error () -> msg


{-| Creation tagger for creation Api functions (used by Api developers).
-}
type alias ApiCreationTagger id error msg =
    id -> ApiCreationResultTagger error msg


{-| Creation tagger for operational Api functions (used by Api developers).
-}
type alias ApiOperationTagger error msg =
    EntityId -> ApiOperationResultTagger error msg


{-| Creation tagger for non-mutating Api functions (used by Api developers).
-}
type alias ApiNonMutatingTagger error msg =
    () -> ApiOperationResultTagger error msg


{-| Custom validation error handler function signature
-}
type alias CustomValidationErrorHandler customValidationError error msg =
    customValidationError -> ApiResult error msg -> msg


commandProcessorConfig : CommandProcessor.Config customValidationError (Msg customValidationError msg)
commandProcessorConfig =
    { routeToMeTagger = CommandProcessorMsg
    , errorTagger = CommandProcessorError
    , logTagger = CommandProcessorLog
    , commandErrorTagger = CommandError
    , commandSuccessTagger = CommandSuccess
    }


initModel : Seed -> ( Model customValidationError error msg, List (Cmd (Msg customValidationError msg)) )
initModel initialSeed =
    let
        ( commandProcessorModel, commandProcessorCmd ) =
            CommandProcessor.init commandProcessorConfig
    in
        ( { seed = initialSeed
          , commands = Dict.empty
          , commandProcessorModel = commandProcessorModel
          }
        , [ commandProcessorCmd ]
        )



-- API


{-| Initialize API
-}
init : Config customValidationError error msg -> Seed -> ( Model customValidationError error msg, Cmd msg )
init config initialSeed =
    let
        ( model, cmds ) =
            initModel initialSeed
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


{-| Command Processor's Msg
-}
type Msg customValidationError msg
    = CommandProcessorMsg (CommandProcessor.Msg customValidationError)
    | CommandProcessorLog ( LogLevel, ( CommandId, String ) )
    | CommandProcessorError ( ErrorType, ( CommandId, String ) )
    | CommandError ( CommandId, CommandError customValidationError )
    | CommandSuccess CommandId
    | Validate msg


{-| Update.
-}
update : Config customValidationError error msg -> Msg customValidationError msg -> Model customValidationError error msg -> ( ( Model customValidationError error msg, Cmd (Msg customValidationError msg) ), List msg )
update config msg model =
    let
        getCommandState commandId =
            Dict.get commandId model.commands ?!= (\_ -> Debug.crash ("Cannot find Command State for commandId:" +-+ commandId))

        removeCommandState model commandId =
            { model | commands = Dict.remove commandId model.commands }

        errorMsgToApiError commandId errorType errorMsg =
            let
                error =
                    Err <| config.errorMsgToError errorType errorMsg
            in
                case (getCommandState commandId).tagger of
                    CreationResult _ tagger ->
                        tagger error

                    OperationResult tagger ->
                        tagger error

                    NonMutatingResult tagger ->
                        tagger error

        updateCommandProcessor =
            updateChildParent (CommandProcessor.update commandProcessorConfig) (update config) .commandProcessorModel CommandProcessorMsg (\model commandProcessorModel -> { model | commandProcessorModel = commandProcessorModel })
    in
        case msg of
            CommandProcessorMsg msg ->
                updateCommandProcessor msg model

            CommandProcessorLog ( logLevel, details ) ->
                ( model ! [], [ config.logTagger ( logLevel, "CommandProcessor:" +-+ details ) ] )

            CommandProcessorError ( errorType, ( commandId, details ) ) ->
                ( removeCommandState model commandId ! [], [ errorMsgToApiError commandId errorType ("CommandProcessor:" +-+ details) ] )

            CommandError ( commandId, error ) ->
                let
                    msg =
                        case error of
                            OperationalCommandError errorMsg ->
                                errorMsgToApiError commandId NonFatalError <| "OperationalCommandError:" +-+ errorMsg

                            EntityValidationCommandError errors ->
                                errorMsgToApiError commandId NonFatalError <| "EntityValidationCommandError:" +-+ (toStringF errors)

                            CustomValidationCommandError customValidationError ->
                                config.customValidationErrorHandler customValidationError (getCommandState commandId).tagger
                in
                    ( removeCommandState model commandId ! [], [ msg ] )

            CommandSuccess commandId ->
                let
                    msg =
                        case (getCommandState commandId).tagger of
                            CreationResult entityId tagger ->
                                tagger <| Ok entityId

                            OperationResult tagger ->
                                tagger <| Ok ()

                            NonMutatingResult tagger ->
                                tagger <| Ok ()
                in
                    ( removeCommandState model commandId ! [], [ msg ] )

            Validate msg ->
                ( model ! [], [ msg ] )


{-| Process creation mutating events.
-}
processCreationMutatingEvents : List MutatingEvent -> String -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiCreationTagger id error msg -> EntityId -> id -> ( Model customValidationError error msg, Cmd msg )
processCreationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId id =
    prepareCommand mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model <| CreationResult entityId <| tagger id


{-| Process operational mutating events.
-}
processOperationMutatingEvents : List MutatingEvent -> String -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    prepareCommand mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model (OperationResult <| tagger entityId)


{-| Process non-mutating events.
-}
processNonMutatingEvents : List Value -> String -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiNonMutatingTagger error msg -> ( Model customValidationError error msg, Cmd msg )
processNonMutatingEvents nonMutatingEvents commandName config dbConnectionInfo initiatorId model tagger =
    prepareNonMutatingCommand nonMutatingEvents commandName config dbConnectionInfo initiatorId model (NonMutatingResult <| tagger ())


{-| Create an entity events
-}
createEntityEvents : String -> List ( PropertyName, Value ) -> Config customValidationError error msg -> Model customValidationError error msg -> Result error ( Model customValidationError error msg, EntityId, List MutatingEvent )
createEntityEvents entityName additionProperties config model =
    step Uuid.uuidGenerator model.seed
        |> (\( uuid, newSeed ) ->
                { model | seed = newSeed }
                    |> (\model ->
                            let
                                entityId =
                                    Uuid.toString uuid

                                mutatingEvents =
                                    [ CreateEntity entityName entityId ]
                                        |> (flip List.append <| List.map (\( name, value ) -> AddProperty entityName entityId name value) additionProperties)
                            in
                                additionProperties
                                    |> List.filter (((==) "") << second)
                                    |> List.map first
                                    |> (\badPropertyNames -> (List.length badPropertyNames == 0) ? ( Ok ( model, entityId, mutatingEvents ), Err <| config.valuesInvalid badPropertyNames ))
                       )
           )


{-| Create an entity
-}
createEntity : String -> String -> List ( PropertyName, Value ) -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiCreationTagger id error msg -> id -> Result error ( Model customValidationError error msg, Cmd msg )
createEntity entityName commandName additionProperties maybeValidation config dbConnectionInfo initiatorId model tagger id =
    createEntityEvents entityName additionProperties config model
        |??> (\( model, entityId, mutatingEvents ) -> Ok <| processCreationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId id)
        ??= Err


{-| Destroy entity events
-}
destroyEntityEvents : String -> EntityId -> List MutatingEvent
destroyEntityEvents entityName entityId =
    [ DestroyEntity entityName entityId ]


{-| Destroy entity.
-}
destroyEntity : String -> String -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
destroyEntity entityName commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    processOperationMutatingEvents (destroyEntityEvents entityName entityId) commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId


{-| Add property events
-}
addPropertyEvents : String -> EntityId -> PropertyName -> Value -> Config customValidationError error msg -> Result error (List MutatingEvent)
addPropertyEvents entityName entityId propertyName value config =
    (value /= "") ? ( Ok [ AddProperty entityName entityId propertyName value ], Err config.valueInvalid )


{-| Add property.
-}
addProperty : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> Value -> Result error ( Model customValidationError error msg, Cmd msg )
addProperty entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId value =
    addPropertyEvents entityName entityId propertyName value config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err


{-| Remove property events
-}
removePropertyEvents : String -> EntityId -> PropertyName -> List MutatingEvent
removePropertyEvents entityName entityId propertyName =
    [ RemoveProperty entityName entityId propertyName ]


{-| Remove property.
-}
removeProperty : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
removeProperty entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    processOperationMutatingEvents (removePropertyEvents entityName entityId propertyName) commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId



-- PRIVATE API


prepareCommand : List MutatingEvent -> String -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiResult error msg -> ( Model customValidationError error msg, Cmd msg )
prepareCommand mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model apiResult =
    let
        toEvent metadata mutatingEvent =
            Mutating mutatingEvent metadata

        metadata =
            Metadata initiatorId commandName

        ( entityValidations, encodedEvents ) =
            ( mutatingEvents |> List.map (\mutatingEvent -> (LockAndValidate << ValidateForMutation <| toEvent metadata mutatingEvent))
            , mutatingEvents |> List.map (encodeEvent << (toEvent metadata))
            )

        ( newCommandProcessorModel, cmd, commandId ) =
            CommandProcessor.process commandProcessorConfig dbConnectionInfo maybeValidation entityValidations encodedEvents model.commandProcessorModel
    in
        ( { model | commandProcessorModel = newCommandProcessorModel, commands = Dict.insert commandId (CommandState <| apiResult) model.commands }, Cmd.map config.routeToMeTagger cmd )


prepareNonMutatingCommand : List Value -> String -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiResult error msg -> ( Model customValidationError error msg, Cmd msg )
prepareNonMutatingCommand nonMutatingEvents commandName config dbConnectionInfo initiatorId model apiResult =
    let
        toEvent metadata value =
            NonMutating value metadata

        metadata =
            Metadata initiatorId commandName

        ( entityValidations, encodedEvents ) =
            ( [], nonMutatingEvents |> List.map (encodeEvent << (toEvent metadata)) )

        ( newCommandProcessorModel, cmd, commandId ) =
            CommandProcessor.process commandProcessorConfig dbConnectionInfo Nothing entityValidations encodedEvents model.commandProcessorModel
    in
        ( { model | commandProcessorModel = newCommandProcessorModel, commands = Dict.insert commandId (CommandState <| apiResult) model.commands }, Cmd.map config.routeToMeTagger cmd )
