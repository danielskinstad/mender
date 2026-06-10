// Copyright 2023 Northern.tech AS
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

#include <mender-update/progress_reader/progress_reader.hpp>

#include <cstdlib>
#include <ctime>
#include <iostream>
#include <vector>

#include <common/io.hpp>

#include <gtest/gtest.h>

namespace io = mender::common::io;

namespace progress = mender::update::progress;

TEST(ProgressReaderTests, RegularRead) {
	std::srand(static_cast<unsigned int>(time(nullptr)));

	std::vector<uint8_t> data(1024 * 100);

	std::generate_n(data.begin(), 1024 * 100, std::ref(std::rand));

	std::string d {data.begin(), data.end()};

	std::shared_ptr<io::StringReader> rdr = std::make_shared<io::StringReader>(d);

	auto reader = progress::Reader(rdr, 1024 * 100);

	testing::internal::CaptureStderr();

	// Read < 1 %
	std::vector<uint8_t> tmp(1024 * 100);
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 10)));

	// Read 5%
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 5 * 1024)));

	// Read < 1%
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 10)));

	// Read 25%
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 20 * 1024)));

	// Read 90%
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 65 * 1024)));

	// Read 100%
	ASSERT_TRUE(reader.Read(tmp.begin(), std::next(tmp.begin(), 10 * 1024)));

	std::string output = testing::internal::GetCapturedStderr();

	EXPECT_EQ(output, "\r0%\r5%\r25%\r90%\r100%");
}

TEST(ProgressReaderTests, ProgressCallback) {
	// Buffer of known size so percentages are deterministic.
	const int64_t size = 1000;
	std::vector<uint8_t> data(static_cast<size_t>(size), 'x');
	std::string d {data.begin(), data.end()};

	std::shared_ptr<io::StringReader> rdr = std::make_shared<io::StringReader>(d);

	std::vector<int> progress;
	auto reader = progress::Reader(
		rdr, size, [&progress](int percentage) { progress.push_back(percentage); });

	testing::internal::CaptureStderr();

	// Read the whole buffer 10 bytes at a time => one callback per 1% increase.
	std::vector<uint8_t> tmp(10);
	for (int i = 0; i < 100; i++) {
		ASSERT_TRUE(reader.Read(tmp.begin(), tmp.end()));
	}

	(void) testing::internal::GetCapturedStderr();

	// One callback per distinct percentage increment, strictly increasing,
	// ending at 100.
	ASSERT_FALSE(progress.empty());
	EXPECT_EQ(progress.front(), 1);
	EXPECT_EQ(progress.back(), 100);
	EXPECT_EQ(progress.size(), 100u);
	for (size_t i = 1; i < progress.size(); i++) {
		EXPECT_GT(progress[i], progress[i - 1]);
	}
}

TEST(ProgressReaderTests, NoCallbackBackwardCompatible) {
	// Two-arg construction must still work and not crash (no callback).
	const int64_t size = 100;
	std::vector<uint8_t> data(static_cast<size_t>(size), 'y');
	std::string d {data.begin(), data.end()};

	std::shared_ptr<io::StringReader> rdr = std::make_shared<io::StringReader>(d);
	auto reader = progress::Reader(rdr, size);

	testing::internal::CaptureStderr();
	std::vector<uint8_t> tmp(static_cast<size_t>(size));
	ASSERT_TRUE(reader.Read(tmp.begin(), tmp.end()));
	(void) testing::internal::GetCapturedStderr();
}
