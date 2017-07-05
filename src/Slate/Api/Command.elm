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
        , addToPropertyListEvents
        , addToPropertyList
        , removeFromPropertyListEvents
        , removeFromPropertyList
        , positionPropertyList
        , addRelationshipEvents
        , addRelationship
        , removeRelationshipEvents
        , removeRelationship
        , addToRelationshipListEvents
        , addToRelationshipList
        , removeFromRelationshipListEvents
        , removeFromRelationshipList
        , positionRelationshipList
        )

{-|
    Common Command code for creating Command APIs.

@docs Config , Model , Msg , ApiResult , ApiCreationTagger , ApiOperationTagger , ApiNonMutatingTagger , ApiCreationResultTagger , ApiOperationResultTagger , ApiNonMutatingResultTagger , CustomValidationErrorHandler , init , update , processCreationMutatingEvents , processOperationMutatingEvents , processNonMutatingEvents , createEntityEvents , createEntity , destroyEntityEvents , destroyEntity , addPropertyEvents , addProperty , removePropertyEvents , removeProperty , addToPropertyListEvents , addToPropertyList , removeFromPropertyListEvents , removeFromPropertyList , positionPropertyList , addRelationshipEvents , addRelationship , removeRelationshipEvents , removeRelationship , addToRelationshipListEvents , addToRelationshipList , removeFromRelationshipListEvents , removeFromRelationshipList , positionRelationshipList
-}

import Tuple exposing (..)
import Dict exposing (Dict)
import Uuid
import Random.Pcg as Random exposing (..)
import StringUtils exposing (..)
import ParentChildUpdate exposing (..)
import Slate.Common.Db exposing (..)
import Slate.Common.Entity exposing (..)
import Slate.Common.Schema exposing (..)
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
    { ownerPath : String
    , connectionRetryMax : Int
    , routeToMeTagger : Msg customValidationError msg -> msg
    , logTagger : LogTagger String msg
    , customValidationErrorHandler : CustomValidationErrorHandler customValidationError error msg
    , errorMsgToError : ErrorType -> String -> error
    , valueInvalid : String -> error
    , valuesInvalid : List ( PropertyName, String ) -> error
    , schemaDict : Dict EntityName EntitySchema
    , queryBatchSize : Maybe Int
    , debug : Bool
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


commandProcessorConfig : Config customValidationError error msg -> CommandProcessor.Config customValidationError (Msg customValidationError msg)
commandProcessorConfig config =
    { connectionRetryMax = config.connectionRetryMax
    , routeToMeTagger = CommandProcessorMsg
    , errorTagger = CommandProcessorError
    , logTagger = CommandProcessorLog
    , commandErrorTagger = CommandError
    , commandSuccessTagger = CommandSuccess
    , schemaDict = config.schemaDict
    , queryBatchSize = config.queryBatchSize
    , debug = config.debug
    }


initModel : Config customValidationError error msg -> Seed -> ( Model customValidationError error msg, List (Cmd (Msg customValidationError msg)) )
initModel config initialSeed =
    let
        ( commandProcessorModel, commandProcessorCmd ) =
            CommandProcessor.init (commandProcessorConfig config)
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
            initModel config initialSeed
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
            Dict.get commandId model.commands ?!= (\_ -> Debug.crash ("Cannot find Command State for commandId:" +-+ "(" ++ config.ownerPath ++ ")" +-+ commandId))

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
            updateChildParent (CommandProcessor.update (commandProcessorConfig config)) (update config) .commandProcessorModel CommandProcessorMsg (\model commandProcessorModel -> { model | commandProcessorModel = commandProcessorModel })
    in
        case msg of
            CommandProcessorMsg msg ->
                updateCommandProcessor msg model

            CommandProcessorLog ( logLevel, details ) ->
                ( model ! [], [ config.logTagger ( logLevel, "CommandProcessor:" +-+ "(" ++ config.ownerPath ++ ")" +-+ details ) ] )

            CommandProcessorError ( errorType, ( commandId, details ) ) ->
                ( removeCommandState model commandId ! [], [ errorMsgToApiError commandId errorType ("CommandProcessor:" +-+ "(" ++ config.ownerPath ++ ")" +-+ details) ] )

            CommandError ( commandId, error ) ->
                let
                    msg =
                        case error of
                            OperationalCommandError errorMsg ->
                                errorMsgToApiError commandId NonFatalError <| "OperationalCommandError:" +-+ "(" ++ config.ownerPath ++ ")" +-+ errorMsg

                            EntityValidationCommandError errors ->
                                errorMsgToApiError commandId NonFatalError <| "EntityValidationCommandError:" +-+ "(" ++ config.ownerPath ++ ")" +-+ (toStringF errors)

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
                                    |> List.map ((flip (,)) "Blank PropertyName")
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


propertyValueMustNotBeBlank : String
propertyValueMustNotBeBlank =
    "Property Value must not be blank"


{-| Add property events
-}
addPropertyEvents : String -> EntityId -> PropertyName -> Value -> Config customValidationError error msg -> Result error (List MutatingEvent)
addPropertyEvents entityName entityId propertyName value config =
    (value /= "") ? ( Ok [ AddProperty entityName entityId propertyName value ], Err <| config.valueInvalid propertyValueMustNotBeBlank )


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


{-| Add to property list events
-}
addToPropertyListEvents : String -> EntityId -> PropertyName -> PropertyId -> Value -> Config customValidationError error msg -> Result error (List MutatingEvent)
addToPropertyListEvents entityName entityId propertyName propertyId value config =
    (value /= "") ? ( Ok [ AddPropertyList entityName entityId propertyName propertyId value ], Err <| config.valueInvalid propertyValueMustNotBeBlank )


{-| Add to property list.
-}
addToPropertyList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> PropertyId -> Value -> Result error ( Model customValidationError error msg, Cmd msg )
addToPropertyList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId propertyId value =
    addToPropertyListEvents entityName entityId propertyName propertyId value config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err


{-| Remove from property list events
-}
removeFromPropertyListEvents : String -> EntityId -> PropertyName -> PropertyId -> List MutatingEvent
removeFromPropertyListEvents entityName entityId propertyName propertyId =
    [ RemovePropertyList entityName entityId propertyName propertyId ]


{-| Remove property.
-}
removeFromPropertyList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> PropertyId -> ( Model customValidationError error msg, Cmd msg )
removeFromPropertyList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId propertyId =
    processOperationMutatingEvents (removeFromPropertyListEvents entityName entityId propertyName propertyId) commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId


invalidPosition : String
invalidPosition =
    "First and last position must be postitive and not equal to each other"


{-| Position property list events
-}
positionPropertyListEvents : String -> EntityId -> PropertyName -> Position -> Config customValidationError error msg -> Result error (List MutatingEvent)
positionPropertyListEvents entityName entityId propertyName position config =
    (first position /= second position && first position >= 0 && second position >= 0) ? ( Ok [ PositionPropertyList entityName entityId propertyName position ], Err <| config.valueInvalid invalidPosition )


{-| Position property list.
-}
positionPropertyList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> Position -> Result error ( Model customValidationError error msg, Cmd msg )
positionPropertyList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId position =
    positionPropertyListEvents entityName entityId propertyName position config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err


relationshipIdMustNotBeBlank : String
relationshipIdMustNotBeBlank =
    "RelationshipId must not be blank"


{-| Add relationship events
-}
addRelationshipEvents : String -> EntityId -> PropertyName -> RelationshipId -> Config customValidationError error msg -> Result error (List MutatingEvent)
addRelationshipEvents entityName entityId propertyName relationshipId config =
    (relationshipId /= "") ? ( Ok [ AddRelationship entityName entityId propertyName relationshipId ], Err <| config.valueInvalid relationshipIdMustNotBeBlank )


{-| Add relationship.
-}
addRelationship : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> RelationshipId -> Result error ( Model customValidationError error msg, Cmd msg )
addRelationship entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId relationshipId =
    addRelationshipEvents entityName entityId propertyName relationshipId config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err


{-| Remove relationship events
-}
removeRelationshipEvents : String -> EntityId -> PropertyName -> List MutatingEvent
removeRelationshipEvents entityName entityId propertyName =
    [ RemoveRelationship entityName entityId propertyName ]


{-| Remove relationship.
-}
removeRelationship : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> ( Model customValidationError error msg, Cmd msg )
removeRelationship entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId =
    processOperationMutatingEvents (removeRelationshipEvents entityName entityId propertyName) commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId


{-| Add to relationship list events
-}
addToRelationshipListEvents : String -> EntityId -> PropertyName -> PropertyId -> RelationshipId -> Config customValidationError error msg -> Result error (List MutatingEvent)
addToRelationshipListEvents entityName entityId propertyName propertyId relationshipId config =
    (relationshipId /= "") ? ( Ok [ AddRelationshipList entityName entityId propertyName propertyId relationshipId ], Err <| config.valueInvalid relationshipIdMustNotBeBlank )


{-| Add to relationship list.
-}
addToRelationshipList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> PropertyId -> RelationshipId -> Result error ( Model customValidationError error msg, Cmd msg )
addToRelationshipList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId propertyId relationshipId =
    addToRelationshipListEvents entityName entityId propertyName propertyId relationshipId config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err


{-| Remove relationship list events
-}
removeFromRelationshipListEvents : String -> EntityId -> PropertyName -> PropertyId -> List MutatingEvent
removeFromRelationshipListEvents entityName entityId propertyName propertyId =
    [ RemoveRelationshipList entityName entityId propertyName propertyId ]


{-| Remove relationship list.
-}
removeFromRelationshipList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> PropertyId -> ( Model customValidationError error msg, Cmd msg )
removeFromRelationshipList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId propertyId =
    processOperationMutatingEvents (removeFromRelationshipListEvents entityName entityId propertyName propertyId) commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId


{-| Position relationship list events
-}
positionRelationshipListEvents : String -> EntityId -> PropertyName -> Position -> Config customValidationError error msg -> Result error (List MutatingEvent)
positionRelationshipListEvents entityName entityId propertyName position config =
    (first position /= second position && first position >= 0 && second position >= 0) ? ( Ok [ PositionRelationshipList entityName entityId propertyName position ], Err <| config.valueInvalid invalidPosition )


{-| Position relationship list.
-}
positionRelationshipList : String -> String -> PropertyName -> Maybe (ValidateTagger (CommandProcessor.Msg customValidationError) customValidationError (Msg customValidationError msg)) -> Config customValidationError error msg -> DbConnectionInfo -> InitiatorId -> Model customValidationError error msg -> ApiOperationTagger error msg -> EntityId -> Position -> Result error ( Model customValidationError error msg, Cmd msg )
positionRelationshipList entityName commandName propertyName maybeValidation config dbConnectionInfo initiatorId model tagger entityId position =
    positionRelationshipListEvents entityName entityId propertyName position config
        |??> (\mutatingEvents -> Ok <| processOperationMutatingEvents mutatingEvents commandName maybeValidation config dbConnectionInfo initiatorId model tagger entityId)
        ??= Err



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
            CommandProcessor.process (commandProcessorConfig config) dbConnectionInfo maybeValidation entityValidations encodedEvents model.commandProcessorModel
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
            CommandProcessor.process (commandProcessorConfig config) dbConnectionInfo Nothing entityValidations encodedEvents model.commandProcessorModel
    in
        ( { model | commandProcessorModel = newCommandProcessorModel, commands = Dict.insert commandId (CommandState <| apiResult) model.commands }, Cmd.map config.routeToMeTagger cmd )
