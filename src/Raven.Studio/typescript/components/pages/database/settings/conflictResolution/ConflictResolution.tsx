﻿import React, { useEffect } from "react";
import { Button, Card, CardBody, Col, Row, UncontrolledTooltip } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useAppDispatch, useAppSelector } from "components/store";
import { NonShardedViewProps } from "components/models/common";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import { HrHeader } from "components/common/HrHeader";
import ConflictResolutionConfigPanel from "components/pages/database/settings/conflictResolution/ConflictResolutionConfigPanel";
import { Switch } from "components/common/Checkbox";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { LoadError } from "components/common/LoadError";
import { LazyLoad } from "components/common/LazyLoad";
import {
    conflictResolutionSelectors,
    conflictResolutionActions,
    ConflictResolutionCollectionConfig,
} from "./store/conflictResolutionSlice";
import { EmptySet } from "components/common/EmptySet";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";

todo("Feature", "Damian", "Remove legacy code");
todo("Other", "Danielle", "Add Info Hub text");

// TODO report google analitics event

export default function ConflictResolution({ db }: NonShardedViewProps) {
    const { databasesService } = useServices();
    // To separate file
    const conflictResolutionDocsLink = useRavenLink({ hash: "QRCNKH" });
    const isDatabaseAdmin =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const dispatch = useAppDispatch();
    const loadStatus = useAppSelector(conflictResolutionSelectors.loadStatus);
    const isResolveToLatest = useAppSelector(conflictResolutionSelectors.isResolveToLatest);
    const collectionConfigs = useAppSelector(conflictResolutionSelectors.collectionConfigs);
    const isDirty = useAppSelector(conflictResolutionSelectors.isDirty);
    const isSomeInEditMode = useAppSelector(conflictResolutionSelectors.isSomeInEditMode);

    useEffect(() => {
        dispatch(conflictResolutionActions.fetchConfig(db));

        return () => {
            dispatch(conflictResolutionActions.reset());
        };
    }, [db, dispatch]);

    const asyncSave = useAsyncCallback(
        () => databasesService.saveConflictSolverConfiguration(db, mapToDto(isResolveToLatest, collectionConfigs)),
        {
            onSuccess: () => dispatch(conflictResolutionActions.saveAll()),
        }
    );

    if (loadStatus === "failure") {
        return (
            <LoadError
                error="Unable to load conflict resolution"
                refresh={() => dispatch(conflictResolutionActions.fetchConfig(db))}
            />
        );
    }

    return (
        <Row className="content-margin gy-sm">
            <Col>
                <AboutViewHeading title="Conflict Resolution" icon="conflicts-resolution" />
                <LazyLoad active={loadStatus === "idle" || loadStatus === "loading"}>
                    {isDatabaseAdmin && (
                        <>
                            <div id="saveConflictResolutionScript" className="d-flex w-fit-content gap-3 mb-3">
                                <ButtonWithSpinner
                                    color="primary"
                                    icon="save"
                                    isSpinning={asyncSave.loading}
                                    onClick={asyncSave.execute}
                                    disabled={!isDirty || isSomeInEditMode}
                                >
                                    Save
                                </ButtonWithSpinner>
                            </div>
                            {isSomeInEditMode && (
                                <UncontrolledTooltip target="saveConflictResolutionScript">
                                    Please finish editing all scripts before saving
                                </UncontrolledTooltip>
                            )}
                        </>
                    )}
                    <div className="mb-3">
                        <HrHeader
                            right={
                                isDatabaseAdmin && (
                                    <div id="addNewScriptButton">
                                        <Button
                                            color="info"
                                            size="sm"
                                            className="rounded-pill"
                                            title="Add a new Conflicts Resolution script"
                                            onClick={() => dispatch(conflictResolutionActions.add())}
                                        >
                                            <Icon icon="plus" />
                                            Add new
                                        </Button>
                                    </div>
                                )
                            }
                            count={collectionConfigs.length}
                        >
                            <Icon icon="documents" />
                            Collection-specific scripts
                        </HrHeader>
                        {collectionConfigs.length > 0 ? (
                            collectionConfigs.map((config) => (
                                <ConflictResolutionConfigPanel
                                    key={config.id}
                                    initialConfig={config}
                                    isDatabaseAdmin={isDatabaseAdmin}
                                />
                            ))
                        ) : (
                            <EmptySet>No scripts have been defined</EmptySet>
                        )}
                    </div>
                    <Card>
                        <CardBody>
                            <Switch
                                color="primary"
                                selected={isResolveToLatest}
                                toggleSelection={() => dispatch(conflictResolutionActions.toggleIsResolveToLatest())}
                                disabled={!isDatabaseAdmin}
                            >
                                If no script was defined for a collection, resolve the conflict using the latest version
                            </Switch>
                        </CardBody>
                    </Card>
                </LazyLoad>
            </Col>
            <Col sm={12} lg={4}>
                <AboutViewAnchored>
                    <AccordionItemWrapper
                        targetId="1"
                        icon="about"
                        color="info"
                        description="Get additional info on this feature"
                        heading="About this view"
                    >
                        <p>Text for Conflicts Resolution</p>
                        <hr />
                        <div className="small-label mb-2">useful links</div>
                        <a href={conflictResolutionDocsLink} target="_blank">
                            <Icon icon="newtab" /> Docs - Conflict Resolution
                        </a>
                    </AccordionItemWrapper>
                </AboutViewAnchored>
            </Col>
        </Row>
    );
}

function mapToDto(
    isResolveToLatest: boolean,
    collectionConfigs: ConflictResolutionCollectionConfig[]
): Raven.Client.ServerWide.ConflictSolver {
    return {
        ResolveToLatest: isResolveToLatest,
        ResolveByCollection: Object.fromEntries(
            collectionConfigs.map((config) => [
                config.name,
                {
                    Script: config.script,
                    LastModifiedTime: config.lastModifiedTime,
                },
            ])
        ),
    };
}
