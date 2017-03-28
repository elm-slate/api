module Slate.Api.Command
    exposing
        ( Config
        , Model
        , Msg(..)
        , ApiResult(..)
        , ApiCreationTagger
        , ApiOperationTagger
        , ApiCreationResultTagger
        , ApiOperationResultTagger
        , CustomValidationErrorHandler
        , init
        , update
        , processMutatingEvents
        , createEntity
        , destroyEntity
        , addProperty
        , removeProperty
        )

{-|
    Common Command code for creating Command APIs.

@docs Config , Model , Msg , ApiResult , ApiCreationTagger , ApiOperationTagger , ApiCreationResultTagger , ApiOperationResultTagger , CustomValidationErrorHandler , init , update , processMutatingEvents , createEntity , destroyEntity , addProperty , removeProperty
-}

import Task
import Time
import Dict exposing (Dict)
import Uuid
import Random.Pcg exposing (Seed, initialSeed, step)
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
    , apiNotInitialized : error
    , valueInvalid : error
    }


type alias CommandDict error msg =
    Dict CommandId (CommandState error msg)


type alias CommandState error msg =
    { tagger : ApiResult error msg
    }


{-| Common Model
-}
type alias Model customValidationError error msg =
    { seed : Maybe Seed
    , commandProcessorModel : CommandProcessor.Model customValidationError (Msg customValidationError msg)
    , commands : CommandDict error msg
    }


{-| Api Result
-}
type ApiResult error msg
    = CreationResult EntityId (ApiCreationResultTagger error msg)
    | OperationResult (ApiOperationResultTagger error msg)


{-| Creation tagger result for creation Api calls (used by App).
-}
type alias ApiCreationResultTagger error msg =
    Result error EntityId -> msg


{-| Creation tagger result for operational Api calls (used by App).
-}
type alias ApiOperationResultTagger error msg =
    Result error () -> msg


{-| Creation tagger for creation Api functions (used by Api developers).
-}
type alias ApiCreationTagger id error msg =
    id -> ApiCreationResultTagger error msg


{-| Creation tagger for operational Api functions (used by Api developers).
-}
type alias ApiOperationTagger error msg =
    EntityId -> ApiOperationResultTagger error msg


{-| Custom validation error handler function signature
-}
type alias CustomValidationErrorHandler customValidationError error msg =
    customValidationError -> ApiResult error msg -> msg


commandProcessorConfig : CommandProcessor.Config customValidationError (Msg customValidationError msg)
commandProcessorConfig =
    { routeToMeTagger = CommandProcessorModule
    , errorTagger = CommandProcessorError
    , logTagger = CommandProcessorLog
    , commandErrorTagger = CommandError
    , commandSuccessTagger = CommandSuccess
    }


initModel : ( Model customValidationError error msg, List (Cmd (Msg customValidationError msg)) )
initModel =
    let
        ( commandProcessorModel, commandProcessorCmd ) =
            CommandProcessor.init commandProcessorConfig
    in
        ( { seed = Nothing
          , commands = Dict.empty
          , commandProcessorModel = commandProcessorModel
          }
        , [ commandProcessorCmd, Time.now |> Task.perform InitSeed ]
        )



-- API


{-| Initialize API
-}
init : Config customValidationError error msg -> ( Model customValidationError error msg, Cmd msg )
init config =
    let
        ( model, cmds ) =
            initModel
    in
        model ! (List.map (Cmd.map config.routeToMeTagger) cmds)


{-| Command Processor's Msg
-}
type Msg customValidationError msg
    = InitSeed Float
    | CommandProcessorModule (CommandProcessor.Msg customValidationError)
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

        updateCommandProcessor =
            updateChildParent (CommandProcessor.update commandProcessorConfig) (update config) .commandProcessorModel CommandProcessorModule (\model commandProcessorModel -> { model | commandProcessorModel = commandProcessorModel })
    in
        case msg of
            InitSeed time ->
                ( { model | seed = Just <| initialSeed <| round time } ! [], [] )

            CommandProcessorModule msg ->
                updateCommandProcessor msg model

            CommandProcessorLog ( logLevel, details ) ->
                ( model ! [], [ config.logTagger <| ( logLevel, "CommandProcessor:" +-+ details ) ] )

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
                    cmd =
                        case (getCommandState commandId).tagger of
                            CreationResult userId tagger ->
                                tagger <| Ok userId

                            OperationResult tagger ->
                                tagger <| Ok ()
                in
                    ( removeCommandState model commandId ! [], [ cmd ] )

            Validate msg ->
                ( model ! [], [ msg ] )


{-| Process operational mutating events (not creation).
-}
processMutatingEvents : List MutatingEvent -> String -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
processMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    let
        ( entityValidations, encodedEvents ) =
            prepareCommand initiatorId commandName mutatingEvents

        ( newCommandProcessorModel, cmd, commandId ) =
            CommandProcessor.process commandProcessorConfig dbConnectionInfo maybeValidation entityValidations encodedEvents model.commandProcessorModel
    in
        ( { model | commandProcessorModel = newCommandProcessorModel, commands = Dict.insert commandId (CommandState <| OperationResult <| tagger entityId) model.commands }, Cmd.map config.routeToMeTagger cmd )


{-| Create an entity
-}
createEntity : String -> String -> List ( PropertyName, Value ) -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiCreationTagger Value error msg -> String -> Result error ( Model customValidationError error msg, Cmd msg )
createEntity entityName commandName additionProperties maybeValidation config dbConnectionInfo initiatorId model tagger value =
    model.seed
        |?> (\seed ->
                let
                    ( uuid, newSeed ) =
                        step Uuid.uuidGenerator seed

                    newModel =
                        { model | seed = Just newSeed }

                    entityId =
                        Uuid.toString uuid

                    mutatingEvents =
                        [ CreateEntity entityName entityId ]
                            |> (flip List.append <| List.map (\( name, value ) -> AddProperty entityName entityId name value) additionProperties)

                    ( entityValidations, encodedEvents ) =
                        prepareCommand initiatorId commandName mutatingEvents

                    ( commandProcessorModel, cmd, commandId ) =
                        CommandProcessor.process commandProcessorConfig dbConnectionInfo maybeValidation entityValidations encodedEvents newModel.commandProcessorModel
                in
                    Ok ( { newModel | commandProcessorModel = commandProcessorModel, commands = Dict.insert commandId (CommandState <| CreationResult entityId <| tagger value) newModel.commands }, Cmd.map config.routeToMeTagger cmd )
            )
        ?= Err config.apiNotInitialized


{-| Destroy entity.
-}
destroyEntity : String -> String -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
destroyEntity entityName commandName config dbConnectionInfo initiatorId model tagger entityId =
    let
        mutatingEvents =
            [ DestroyEntity entityName entityId
            ]

        ( entityValidations, encodedEvents ) =
            prepareCommand initiatorId commandName mutatingEvents

        ( commandProcessorModel, cmd, commandId ) =
            CommandProcessor.process commandProcessorConfig dbConnectionInfo Nothing entityValidations encodedEvents model.commandProcessorModel
    in
        ( { model | commandProcessorModel = commandProcessorModel, commands = Dict.insert commandId (CommandState <| OperationResult <| tagger entityId) model.commands }, Cmd.map config.routeToMeTagger cmd )


{-| Add property.
-}
addProperty : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> Value -> Result error ( Model customValidationError error msg, Cmd msg )
addProperty entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId value =
    (value /= "") ? ( Ok <| processMutatingEvents [ AddProperty entityName entityId propertyName value ] commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId, Err config.valueInvalid )


{-| Remove property.
-}
removeProperty : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
removeProperty entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    processMutatingEvents [ RemoveProperty entityName entityId propertyName ] commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId



-- PRIVATE API


prepareCommand : InitiatorId -> String -> List MutatingEvent -> ( List EntityLockAndValidation, List String )
prepareCommand initiatorId command mutatingEvents =
    let
        toEvent : Metadata -> MutatingEvent -> Event
        toEvent metadata mutatingEvent =
            Mutating mutatingEvent metadata

        metadata =
            Metadata initiatorId command

        ( entityValidations, encodedEvents ) =
            ( mutatingEvents |> List.map (\mutatingEvent -> (LockAndValidate << ValidateForMutation <| toEvent metadata mutatingEvent))
            , mutatingEvents |> List.map (encodeMutatingEvent << (toEvent metadata))
            )
    in
        ( entityValidations, encodedEvents )
