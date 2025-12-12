import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export default function DocumentSchemaPlaygroundAboutView() {
    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                icon="about"
                color="info"
                description="Get additional info on this feature"
                heading="About this view"
            >
                <div>
                    <ul>
                        <li>
                            In this playground, you can test how your documents validate against sample JSON schemas.
                        </li>
                        <li className="mt-2">
                            This is a safe, temporary workspace. No changes are made to your existing documents or to
                            your saved schema definitions.
                        </li>
                        <li className="mt-2">
                            Define one or more sample schemas per collection, run the test, and review any validation
                            errors found.
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
